package org.heigit.ohsome.parquet;

import org.locationtech.jts.geom.Envelope;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public class GeoParquet<T> {

    public static class GeoParquetBuilder<T> {
        private String primaryColumn;
        private final List<Column<T>> columns = new ArrayList<>();

        public GeoParquetBuilder() {
        }

        public GeoParquetBuilder<T> column(String name, Encoding encoding, EnumSet<GeometryType> geometryTypes, Function<T, Envelope> bbox) {
            if (primaryColumn == null) {
                primaryColumn = name;
            }
            return column(name, encoding, geometryTypes, null, bbox);
        }

        public GeoParquetBuilder<T> column(String name, Encoding encoding, EnumSet<GeometryType> geometryTypes, String covering, Function<T, Envelope> bbox) {
            columns.add(new Column<>(name, encoding, geometryTypes, covering, bbox));
            return this;
        }

        public GeoParquet<T> build() {
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
            return "\"%s\": { \"encoding\": \"%s\", \"bbox\": %s%s, \"geometry_types\": %s }".formatted(
                    name,
                    encoding.schema(),
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

