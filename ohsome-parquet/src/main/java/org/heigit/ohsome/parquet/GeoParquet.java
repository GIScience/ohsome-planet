package org.heigit.ohsome.parquet;

import org.locationtech.jts.geom.Envelope;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public class GeoParquet<T> {

    private static final String CRS_EPSG_4326 = """
             {
                     "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
                     "type": "GeographicCRS",
                     "name": "WGS 84",
                     "datum_ensemble": {
                       "name": "World Geodetic System 1984 ensemble",
                       "members": [
                         { "name": "World Geodetic System 1984 (Transit)", "id": { "authority": "EPSG", "code": 1166 }},
                         { "name": "World Geodetic System 1984 (G730)",    "id": { "authority": "EPSG", "code": 1152 }},
                         { "name": "World Geodetic System 1984 (G873)",    "id": { "authority": "EPSG", "code": 1153 }},
                         { "name": "World Geodetic System 1984 (G1150)",   "id": { "authority": "EPSG", "code": 1154 }},
                         { "name": "World Geodetic System 1984 (G1674)",   "id": { "authority": "EPSG", "code": 1155 }},
                         { "name": "World Geodetic System 1984 (G1762)",   "id": { "authority": "EPSG", "code": 1156 }},
                         { "name": "World Geodetic System 1984 (G2139)",   "id": { "authority": "EPSG", "code": 1309 }},
                         { "name": "World Geodetic System 1984 (G2296)",   "id": { "authority": "EPSG", "code": 1383 }},
                       ],
                       "ellipsoid": { "name": "WGS 84", "semi_major_axis": 6378137, "inverse_flattening": 298.257223563 },
                       "accuracy": "2.0",
                       "id": {"authority": "EPSG", "code": 6326}
                     },
                     "coordinate_system": {
                       "subtype": "ellipsoidal",
                       "axis": [
                         { "name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree" },
                         { "name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree" },
                       ]
                     },
                     "scope": "Horizontal component of 3D system.",
                     "area": "World.",
                     "bbox": { "south_latitude": -90, "west_longitude": -180, "north_latitude": 90, "east_longitude": 180 },
                     "id": {"authority": "EPSG", "code": 4326},
                   }""";

    public static class GeoParquetBuilder<T> {
        private final List<Column<T>> columns = new ArrayList<>();

        public GeoParquetBuilder() {
        }

        public GeoParquetBuilder<T> column(String name, Encoding encoding, EnumSet<GeometryType> geometryTypes, Function<T, Envelope> bbox) {
            return column(name, encoding, geometryTypes, null, bbox);
        }

        public GeoParquetBuilder<T> column(String name, Encoding encoding, EnumSet<GeometryType> geometryTypes, String covering, Function<T, Envelope> bbox) {
            columns.add(new Column<>(name, encoding, geometryTypes, covering, bbox));
            return this;
        }

        public GeoParquet<T> build(String primaryColumn) {
            return new GeoParquet<>(primaryColumn, columns.stream()
                    .map(column -> new Column<>(column.name, column.encoding, column.geometryTypes, column.covering, column.extend))
                    .toList());
        }
    }

    public static <T> GeoParquetBuilder<T> builder() {
        return new GeoParquetBuilder<>();
    }

    // "^(WKB|point|linestring|polygon|multipoint|multilinestring|multipolygon)$"
    public enum Encoding {
        WKB,
        Point,
        LineString,
        Polygon,
        MultiPoint,
        MultiLineString,
        MultiPolygon;

        private String schema() {
            return "WKB".equals(name()) ? name() : name().toLowerCase();
        }

    }

    // "^(GeometryCollection|(Multi)?(Point|LineString|Polygon))( Z)?$"
    public enum GeometryType {
        Point, LineString, Polygon,
        MultiPoint, MultiLineString, MultiPolygon,
        GeometryCollection
    }

    public record Column<T>(String name, Encoding encoding, Set<GeometryType> geometryTypes, String covering,
                            Function<T, Envelope> extend) {

        private String coveringString() {
            var bbox = Stream.of("xmin", "ymin", "xmax", "ymax")
                    .map(i -> "\"%s\": [ \"%s\", \"%s\" ]".formatted(i, covering, i))
                    .collect(joining(", "));
            return "\"bbox\": { %s }".formatted(bbox);
        }

        private String geometryTypeString() {
            return geometryTypes.stream()
                    .map(type -> "\"%s\"".formatted(type.name()))
                    .collect(joining(", ", "[", "]"));
        }

        public String print(Envelope bbox) {
            return """
                    "%s": {
                          "encoding": "%s",
                          "crs": %s,
                          "bbox": %s%s,
                          "geometry_types": %s
                        }""".formatted(
                    name,
                    encoding.schema(),
                    CRS_EPSG_4326,
                    "[%s, %s, %s, %s]".formatted(bbox.getMinX(), bbox.getMinY(), bbox.getMaxX(), bbox.getMaxY()),
                    covering == null ? "" : ", \"covering\": { %s }".formatted(coveringString()),
                    geometryTypeString());
        }
    }

    private final String primaryColumn;
    private final List<Column<T>> columns;

    private GeoParquet(String primaryColumn, List<Column<T>> columns) {
        this.primaryColumn = primaryColumn;
        this.columns = columns;
    }

    public List<Column<T>> getColumns() {
        return columns;
    }

    private String columnsSchema(Map<String, Envelope> columnBBox) {
        var empty = new Envelope();
        return columns.stream()
                .map(column -> column.print(columnBBox.getOrDefault(column.name(), empty)))
                .collect(joining(",\n    "));
    }

    public String schema(Map<String, Envelope> columnBBox) {
        return """
                {
                  "version": "1.1.0",
                  "primary_colum": "%s",
                  "columns": {
                    %s
                  }
                }""".formatted(primaryColumn, columnsSchema(columnBBox));
    }
}

