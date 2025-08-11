package org.heigit.ohsome.contributions.contrib;

import org.heigit.ohsome.contributions.avro.*;
import org.heigit.ohsome.contributions.util.XZCode;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMType;
import org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner;
import org.heigit.ohsome.contributions.util.AbstractIterator;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBWriter;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;

import static java.util.function.Predicate.not;
import static org.heigit.ohsome.contributions.util.GeometryTools.areaOf;
import static org.heigit.ohsome.contributions.util.GeometryTools.lengthOf;

public class ContributionsAvroConverter extends AbstractIterator<Optional<Contrib>> {
    private static final Instant VALID_TO = Instant.parse("2222-01-01T00:00:00Z");
    private static final Set<String> COLLECTION_TYPES = Set.of(Geometry.TYPENAME_GEOMETRYCOLLECTION);
    private static final XZCode XZ_CODE = new XZCode(16);

    private final OSMType type;
    private final Contributions contributions;
    private final Function<Long, ContribChangeset> changesets;
    private final Contrib.Builder builder = Contrib.newBuilder();
    private final ContribUser.Builder userBuilder = ContribUser.newBuilder();
    private final Centroid.Builder centroidBuilder = Centroid.newBuilder();
    private final ContribXZCode.Builder xzCodeBuilder = ContribXZCode.newBuilder();
    private final BBox.Builder bboxBuilder = BBox.newBuilder();
    private final Member.Builder memberBuilder = Member.newBuilder();


    private final WKBWriter wkb = new WKBWriter();
    private final SpatialJoiner countryJoiner;

    private int minorVersion;
    private int edits;
    private Geometry geometryBefore;
    private double areaBefore;
    private double lengthBefore;

    public ContributionsAvroConverter(Contributions contributions, Function<Long, ContribChangeset> changesets, SpatialJoiner countryJoiner) {
        this.contributions = contributions;
        this.changesets = changesets;
        this.type = contributions.type();
        this.countryJoiner = countryJoiner;
        builder.setOsmType(type.toString());
        builder.setOsmId(contributions.id());
    }

    @Override
    protected Optional<Contrib> computeNext() {
        var buildTime = System.nanoTime();
        if (!contributions.hasNext()) {
            return endOfData();
        }

        var contributionBefore = Optional.ofNullable(contributions.prev());
        var contribution = contributions.next();
        var contributionNext = Optional.of(contributions).filter(Contributions::hasNext).map(Contributions::peek);

        // skip minor changes for the same changeset
        while (contributionNext.isPresent()
               && contributionNext.get().entity().version() == contribution.entity().version()
               && contributionNext.get().changeset() == contribution.changeset()) {
            contributionBefore = Optional.of(contribution);
            contribution = contributions.next();
            contributionNext = Optional.of(contributions).filter(Contributions::hasNext).map(Contributions::peek);
        }

        var entity = contribution.entity();
        var entityBefore = contributionBefore.map(Contribution::entity);

        var status = "latest";
        if (!entity.visible()) {
            status = "deleted";
        } else if (contributionNext.isPresent()) {
            status = "history";
        }

        if (entity.version() != entityBefore.map(OSMEntity::version).orElse(-1)) {
            minorVersion = 0;
        } else {
            minorVersion++;
        }

        builder.setOsmVersion(entity.version());
        builder.setOsmMinorVersion(minorVersion);
        builder.setOsmEdits(++edits);
        builder.setOsmLastEdit(contributionBefore.map(Contribution::timestamp).orElse(null));

        builder.setValidFrom(contribution.timestamp());
        builder.setValidTo(contributionNext.map(Contribution::timestamp).orElse(VALID_TO));

        builder.setUserBuilder(userBuilder.setId(contribution.userId()).setName(contribution.user()));
        builder.setChangeset(changesets.apply(contribution.changeset()));

        builder.setTags(Map.copyOf(entity.tags()));
        builder.setTagsBefore(Map.copyOf(entityBefore.map(OSMEntity::tags).orElse(Map.of())));

        var geometry = !entity.visible() ? geometryBefore : ContributionGeometry.geometry(contribution);

        final double area;
        final double length;
        if (geometry != null && !geometry.isEmpty()) {
            var env = setBBoxCentroidAndXZ(geometry);
            var geometryType = geometry.getGeometryType();
            builder.setGeometryType(geometry.getGeometryType());
            if (COLLECTION_TYPES.contains(geometryType)) {
                // only store bbox for collection types!
                builder.setGeometry(wkb(ContributionGeometry.geometry(env)));
            } else {
                builder.setGeometry(wkb(geometry));
            }

            area = areaOf(geometry);
            length = lengthOf(geometry);
            builder.setCountries(List.copyOf(countryJoiner.join(geometry)));
        } else {
            builder.clearBbox();
            builder.clearCentroid();
            builder.setXzcodeBuilder(xzCodeBuilder.setLevel(-1).setCode(0));
            var collection = ContributionGeometry.relGeometryCollection(contribution);
            if (!collection.isEmpty()) {
                setBBoxCentroidAndXZ(collection);
            }

            builder.clearGeometryType();
            if (geometry != null) {
                builder.setGeometryType(geometry.getGeometryType());
            }
            builder.clearGeometry();
            area = 0.0;
            length = 0.0;
            status = "invalid";
        }

        builder.setArea(area);
        builder.setAreaDelta(area - areaBefore);
        areaBefore = area;
        builder.setLength(length);
        builder.setLengthDelta(length - lengthBefore);
        lengthBefore = length;

        builder.setStatus(status);
        var contribTypes = new ArrayList<String>();
        if (!entity.visible()) {
            contribTypes.add("DELETION");
        } else if (!entityBefore.map(OSMEntity::visible).orElse(false)) {
            contribTypes.add("CREATION");
        } else {
            if (entityBefore.map(OSMEntity::tags).filter(not(entity.tags()::equals)).isEmpty()) {
                contribTypes.add("TAG");
            }
            if (!Objects.equals(geometryBefore, geometry)) {
                contribTypes.add("GEOMETRY");
            }
        }
        builder.setContribType(String.join("_", contribTypes));

        builder.clearRefs();
        builder.clearMembers();

        if (type == OSMType.WAY) {
            builder.setRefs(((OSMEntity.OSMWay) entity).refs());
        } else if (type == OSMType.RELATION) {
            builder.setMembers(contribution.members().stream().map(this::member).toList());
        }
        geometryBefore = geometry;
        return Optional.of(builder.setBuildTime(System.nanoTime() - buildTime).build());
    }

    private Envelope setBBoxCentroidAndXZ(Geometry geometry) {
        var env = geometry.getEnvelopeInternal();
        var centroid = geometry.getCentroid();
        builder.setBboxBuilder(bboxBuilder
                .setXmin(env.getMinX())
                .setYmin(env.getMinY())
                .setXmax(env.getMaxX())
                .setYmax(env.getMaxY()));
        builder.setCentroidBuilder(centroidBuilder
                .setX(centroid.getX())
                .setY(centroid.getY()));
        var xz = XZ_CODE.getId(env.getMinX(), env.getMinY(), env.getMaxX(), env.getMaxY());
        builder.setXzcodeBuilder(xzCodeBuilder.setLevel(xz.level()).setCode(xz.code()));
        return env;
    }

    private Member member(Contribution.ContribMember contribMember) {
        memberBuilder.setId(contribMember.id())
                .setType(contribMember.type().toString())
                .setRole(contribMember.role());
        var contrib = contribMember.contrib();
        if (contrib != null) {
            memberBuilder
                    .setGeometryType(geometry(contrib).getGeometryType())
                    .setGeometry(wkb(contrib));
        } else {
            memberBuilder
                    .setGeometryType(null)
                    .setGeometry(null);
        }
        return memberBuilder.build();
    }

    private Geometry geometry(Contribution contribution) {
        return contribution.data("geometry", ContributionGeometry::geometry);
    }

    private ByteBuffer wkb(Contribution contribution) {
        return contribution.data("wkb", this::contributionWkb);
    }

    private ByteBuffer contributionWkb(Contribution contribution) {
        return wkb(geometry(contribution));
    }

    private ByteBuffer wkb(Geometry geometry) {
        return ByteBuffer.wrap(wkb.write(geometry));
    }
}
