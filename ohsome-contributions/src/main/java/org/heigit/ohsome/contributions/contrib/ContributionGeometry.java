package org.heigit.ohsome.contributions.contrib;

import com.google.common.base.Predicates;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.geometry.GeometryBuilder;
import org.heigit.ohsome.osm.geometry.GeometryBuilderException;
import org.locationtech.jts.geom.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Predicate;

import static java.util.Optional.ofNullable;
import static org.heigit.ohsome.osm.OSMType.RELATION;
import static org.heigit.ohsome.osm.OSMType.WAY;

public class ContributionGeometry {

    public static final Map<String, Predicate<String>> polygonFeatures;
    private static final GeometryFactory geometryFactory = new GeometryFactory();
    private static final Geometry EMPTY_POINT = geometryFactory.createEmpty(0);
    private static final int MEMBERS_THRESHOLD = 500;

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
        return switch (contribution.entity().type()) {
            case NODE -> nodeGeometry(contribution);
            case WAY -> wayGeometry(contribution);
            case RELATION -> relGeometry(contribution);
        };
    }

    public static boolean relIsMultipolygon(Contribution contribution) {
        if (contribution.entity().type() != RELATION) {
            return false;
        }

        if (contribution.members().size() > MEMBERS_THRESHOLD) {
            return false;
        }
        var type = contribution.entity().tags().getOrDefault("type", "");
        return "multipolygon".equalsIgnoreCase(type) || "boundary".equalsIgnoreCase(type);
    }

    public static Geometry relGeometry(Contribution contribution) {
        if (relIsMultipolygon(contribution)) {
            return relGeometryMultiPolygon(contribution);
        }
        return relGeometryCollection(contribution);
    }

    public static Geometry relGeometryMultiPolygon(Contribution contribution) {
        var ways = contribution.members().stream().filter(member -> member.type().equals(WAY) && member.contrib()!=null).toList();
        var outer = ways.stream()
                .filter(member -> "outer".equals(member.role()) || member.role().isBlank())
                .map(member -> member.contrib().data("geometry", ContributionGeometry::geometry))
                .map(geometry -> Arrays.asList(geometry.getCoordinates()))
                .toList();
        var inner = ways.stream()
                .filter(member -> "inner".equals(member.role()))
                .map(member -> member.contrib().data("geometry", ContributionGeometry::geometry))
                .map(geometry -> Arrays.asList(geometry.getCoordinates()))
                .toList();
        try {
            var geometry = GeometryBuilder.buildMultiPolygon(outer, inner);
            if (geometry.isValid()) {
                return geometry;
            }
        } catch (GeometryBuilderException ignored) {
        } catch (Exception e) {
//            System.err.println("Failed building multipolygon: " + e.getMessage());
        }
        return geometryFactory.createMultiPolygon();
    }

    public static Geometry relGeometryCollection(Contribution contribution) {
        var geometries = contribution.members().stream()
                .map(ContribMember::contrib)
                .filter(Objects::nonNull)
                .map(member -> member.data("geometry", ContributionGeometry::geometry))
                .filter(Predicate.not(Geometry::isEmpty))
                .toArray(Geometry[]::new);
        return geometryFactory.createGeometryCollection(geometries);
//        var types = Arrays.stream(geometries).map(Geometry::getGeometryType).collect(Collectors.toSet());
//        if (types.size() != 1) {
//            return geometryFactory.createGeometryCollection(geometries);
//        }
//        var type = types.iterator().next();
//        return switch (type) {
//            case Geometry.TYPENAME_LINESTRING -> geometryFactory.createMultiLineString(Arrays.stream(geometries)
//                            .map(LineString.class::cast)
//                            .toArray(LineString[]::new));
//            case Geometry.TYPENAME_POLYGON -> geometryFactory.createMultiPolygon(Arrays.stream(geometries)
//                            .map(Polygon.class::cast)
//                            .toArray(Polygon[]::new));
//            case Geometry.TYPENAME_POINT -> geometryFactory.createMultiPoint(Arrays.stream(geometries)
//                            .map(Point.class::cast)
//                            .toArray(Point[]::new));
//            default -> throw new IllegalStateException("unknown single geometry_type: " + type);
//        };
    }

    public static Geometry wayGeometry(Contribution contribution) {
        var coordinates = contribution.members().stream()
                .map(ContribMember::contrib)
                .filter(Objects::nonNull)
                .map(Contribution::entity)
                .filter(OSMEntity::visible)
                .map(OSMNode.class::cast)
                .filter(Predicate.not(ContributionGeometry::invalid))
                .map(ContributionGeometry::coordinate)
                .toArray(Coordinate[]::new);

        if (isArea(contribution) && isValidLineRing(coordinates)) {
            return geometryFactory.createPolygon(coordinates);
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
