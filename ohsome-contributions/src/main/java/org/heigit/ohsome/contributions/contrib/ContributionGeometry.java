package org.heigit.ohsome.contributions.contrib;

import com.google.common.base.Predicates;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.geometry.assembler.GeometryAssembler;
import org.locationtech.jts.geom.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Predicate;

import static java.util.Optional.ofNullable;
import static org.heigit.ohsome.osm.OSMType.WAY;

public class ContributionGeometry {
    private static final Logger logger = LoggerFactory.getLogger(ContributionGeometry.class);

    public static final Map<String, Predicate<String>> polygonFeatures;
    private static final GeometryFactory geometryFactory = new GeometryFactory();
    private static final Geometry EMPTY_POINT = geometryFactory.createEmpty(0);

    static {
        var map = new HashMap<String, Predicate<String>>();
        try (var lines = new BufferedReader(new InputStreamReader(ofNullable(ContributionGeometry.class.getResourceAsStream("/polygon_features.csv"))
                .orElseThrow(() -> new IllegalStateException("could not find polygon_features.csv as resources!")))).lines()) {
            lines.skip(1)
                    .map(line -> line.split(";"))
                    .forEach(row -> {
                        var key = row[0].strip().toLowerCase();
                        var type = row[1].strip().toLowerCase();
                        var values = Set.of(row.length == 3 ? Arrays.stream(row[2].split(","))
                                .map(String::strip)
                                .map(String::toLowerCase)
                                .toArray(String[]::new) : new String[0]);
                        Predicate<String> test = switch (type) {
                            case "all" -> Predicates.alwaysTrue();
                            case "whitelist" -> values::contains;
                            case "blacklist" -> Predicate.not(values::contains);
                            default -> throw new IllegalStateException(
                                    "not accepted polygon_feature row! " + Arrays.toString(row));
                        };
                        map.put(key, test);
                    });
            polygonFeatures = Map.copyOf(map);
        }
    }

    private ContributionGeometry() {
        // utility class
    }

    public static boolean testPolygonFeature(String key, String value) {
        return polygonFeatures.getOrDefault(key, Predicates.alwaysFalse()).test(value);
    }

    public static Geometry geometry(Contribution contribution) {
        return geometry(contribution, true);
    }
    public static Geometry geometry(Contribution contribution, boolean latest) {
        return switch (contribution.entity().type()) {
            case NODE -> nodeGeometry(contribution);
            case WAY -> wayGeometry(contribution);
            case RELATION -> relGeometry(contribution, latest);
        };
    }

    public static boolean relIsMultipolygon(Contribution contribution) {
        var type = contribution.entity().tags().getOrDefault("type", "");
        return "multipolygon".equalsIgnoreCase(type) || "boundary".equalsIgnoreCase(type);
    }

    /*

    https://osm.org/relation/9326283  // 1.02h
    https://osm.org/relation/11946074 // 1.02h
    https://osm.org/relation/9323456  // 1.03h
    https://osm.org/relation/13663366 // 1.03h
    https://osm.org/relation/9428957  // 1.14h
    https://osm.org/relation/9381668  // 1.18h
    https://osm.org/relation/9382300  // 1.42h
    https://osm.org/relation/3870917  // 1.6h
    https://osm.org/relation/1626722  // 1.63h
    https://osm.org/relation/6677259  // 1.83h
    https://osm.org/relation/9488835  // 1.95h
    https://osm.org/relation/4016746  // 2.1h
    https://osm.org/relation/9350128  // 2.1h
    https://osm.org/relation/4594226  // 2.5h
    https://osm.org/relation/6038068  // Großbritannien > 3h
    https://osm.org/relation/5446634  // 5.1h
    https://osm.org/relation/280282   // Nuba-see > 5h

     */
    private static final Set<Long> IGNORED_MULTIPOLYGONS = Set.of( -1L
        ,   102740L   //
        ,   280282L   // Nuba-see > 5h
        ,   349348L
        ,  1205151L
        ,  1626722L  // 1.63h
        ,  1754729L
        ,  3723592L
        ,  3870917L  // 1.6h
        ,  4016746L  // 2.1h
        ,  4095122L
        ,  4594226L  // 2.5h
        ,  5446634L  // 5.1h
        ,  5631846L
        ,  6038068L  // Großbritannien > 3h
        ,  6677259L  // 1.83h
        ,  7379046L
        ,  9323456L  // 1.03h
        ,  9326283L  // 1.02h
        ,  9350128L  // 2.1h
        ,  9381668L  // 1.18h
        ,  9382300L  // 1.42h
        ,  9428957L  // 1.14h
        ,  9488835L  // 1.95h
        , 11946074L // 1.02h
        , 13663366L // 1.03h
    );

    public static Geometry relGeometry(Contribution contribution, boolean latest) {
        if (relIsMultipolygon(contribution) && (latest || contribution.members().size() <= 1000 || !IGNORED_MULTIPOLYGONS.contains(contribution.entity().id()))) {
            var geom = relGeometryMultiPolygon(contribution);
            if (!geom.isEmpty()) {
                return geom;
            }
        }
        return relGeometryCollection(contribution);
    }

    private static LineString toLineString(Contribution.ContribMember member) {
        var geometry = member.contrib().data("geometry", ContributionGeometry::geometry);
        if (geometry instanceof LineString lineString) {
            return lineString;
        } else if (geometry instanceof Polygon polygon) {
            return polygon.getExteriorRing();
        } else {
            return geometryFactory.createLineString();
        }
    }

    public static Geometry relGeometryMultiPolygon(Contribution contribution) {
        var ways = new Long2ObjectOpenHashMap<LineString>();
        var inner = new LongOpenHashSet();

        contribution.members().stream()
                .filter(member -> member.type().equals(WAY) && member.contrib() != null)
                .forEach(contrib -> {
                    var lineString = toLineString(contrib);
                    if (!lineString.isEmpty()) {
                        if ("inner".equals(contrib.role())){
                            inner.add(contrib.id());
                        }
                        ways.computeIfAbsent(contrib.id(), x -> lineString);
                    }
                });

        try {
            var assembler = new GeometryAssembler(contribution.entity().type(), contribution.entity().id(), contribution.entity().version(), contribution.timestamp());
            var geometry = assembler.assemble(ways, inner);
            if (geometry != null) {
              if (geometry.isValid()) return geometry;

                //logger.debug("Invalid geometry for relation {}: {}", contribution.entity().id(), contribution.timestamp());
            }
        } catch (Exception ignored) {
            // fallback to empty geometry
        }
        return geometryFactory.createMultiPolygon();
    }

    public static Geometry relGeometryCollection(Contribution contribution) {
        var geometries = contribution.members().stream()
                .map(Contribution.ContribMember::contrib)
                .filter(Objects::nonNull)
                .map(member -> member.data("geometry", ContributionGeometry::geometry))
                .filter(Predicate.not(Geometry::isEmpty))
                .toArray(Geometry[]::new);
        return geometryFactory.createGeometryCollection(geometries);
    }

    public static Geometry wayGeometry(Contribution contribution) {
        var coordinates = contribution.members().stream()
                .map(Contribution.ContribMember::contrib)
                .filter(Objects::nonNull)
                .map(Contribution::entity)
                .filter(OSMEntity::visible)
                .map(OSMNode.class::cast)
                .filter(Predicate.not(ContributionGeometry::invalid))
                .map(ContributionGeometry::coordinate)
                .toArray(Coordinate[]::new);

        if (isArea(contribution) && isValidLineRing(coordinates)) {
            var geom = geometryFactory.createPolygon(coordinates);
            if (geom.isValid()) {
                return geom;
            }
        }

        if (isValidLineString(coordinates)) {
            return geometryFactory.createLineString(coordinates);
        }
        return geometryFactory.createPoint(coordinates[0]);
    }

    private static boolean isValidLineString(Coordinate[] coordinates) {
        return coordinates.length == 0 || coordinates.length >= LineString.MINIMUM_VALID_SIZE;
    }

    private static boolean isValidLineRing(Coordinate[] coordinates) {
        return coordinates.length == 0 || (coordinates.length >= LinearRing.MINIMUM_VALID_SIZE && coordinates[0].equals2D(coordinates[coordinates.length - 1]));
    }

    public static boolean isArea(Contribution contribution) {
        var tags = contribution.entity().tags();
        if ("no".equalsIgnoreCase(tags.get("area"))) {
            return false;
        }
        var members = contribution.members();
        return members.size() > 2 &&
               members.getFirst().id() == members.getLast().id() &&
               tags.entrySet().stream().anyMatch(ContributionGeometry::isPolygonFeature);
    }

    public static boolean isPolygonFeature(Entry<String, String> tag) {
        return testPolygonFeature(tag.getKey(), tag.getValue());
    }


    public static Geometry nodeGeometry(Contribution contribution) {
        var entity = (OSMNode) contribution.entity();
        if (!entity.visible() || invalid(entity)) {
            return EMPTY_POINT;
        }
        return geometryFactory.createPoint(new Coordinate(entity.lon(), entity.lat()));
    }

    public static boolean invalid(OSMNode node) {
        return node.lon() < -180.0 || node.lon() > 180.0 || node.lat() < -90.0 || node.lat() > 90.0;
    }

    public static Coordinate coordinate(OSMNode node) {
        return new Coordinate(node.lon(), node.lat());
    }

    public static Geometry geometry(Envelope env) {
        return geometryFactory.toGeometry(env);
    }
}
