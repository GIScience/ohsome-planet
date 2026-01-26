package org.heigit.ohsome.parquet;

import org.locationtech.jts.geom.Envelope;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public class GeoParquet<T> {

    public static class GeoParquetBuilder<T> {
        private final String primaryColumn;
        private final List<Column<T>> columns = new ArrayList<>();

        public GeoParquetBuilder(String  primaryColumn) {
            this.primaryColumn =  primaryColumn;
        }

        public GeoParquetBuilder<T> column(String name, Encoding encoding, EnumSet<GeometryType> geometryTypes, Function<T, Envelope> bbox) {
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

    public static <T> GeoParquetBuilder<T> builder(String  primaryColumn) {
        return new GeoParquetBuilder<>(primaryColumn);
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
                            Envelope bbox, Function<T, Envelope> extend) {
        public Column(String name, Encoding encoding, Set<GeometryType> geometryTypes, String covering, Function<T, Envelope> extend) {
            this(name, encoding, geometryTypes, covering, new Envelope(), extend);
        }

        void extend(T record) {
            bbox.expandToInclude(extend.apply(record));
        }

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

        public String print() {
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


    public void update(T record) {
        columns.forEach(column -> column.extend(record));
    }

    public List<Column<T>> getColumns() {
        return columns;
    }

    private String columnsSchema() {
        return columns.stream()
                .map(Column::print)
                .collect(joining(",\n    "));
    }

    public String schema() {
        return """
                {
                  "version": "1.1.0",
                  "primary_colum": "%s",
                  "columns": {
                    %s
                  }
                }""".formatted(primaryColumn, columnsSchema());
    }
}

