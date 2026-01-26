package org.heigit.ohsome.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.apache.parquet.schema.LogicalTypeAnnotation.geometryType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

public class AvroGeoParquetWriter {

    private AvroGeoParquetWriter() {
        // utility class
    }

    public static <T> ParquetWriter<T> openWriter(Path path, Schema schema, GeoParquet<T> geoParquet, Consumer<AvroGeoParquetBuilder<T>> configuration) throws IOException {
        var builder = builder(path, schema, geoParquet);
        configuration.accept(builder);
        return builder.build();
    }

    public static <T> AvroGeoParquetBuilder<T> builder(Path path,Schema schema, GeoParquet<T> geoParquet) {
        return new AvroGeoParquetBuilder<>(path, schema, geoParquet);
    }


    public static class AvroGeoParquetBuilder<T> extends ParquetWriter.Builder<T,AvroGeoParquetBuilder<T>> {
        private final GeoParquet<T> geoParquet;
        private final Map<String, String> extraMetadata = new HashMap<>();
        private final Schema schema;

        protected AvroGeoParquetBuilder(Path path, Schema schema, GeoParquet<T> geoParquet) {
            super(new LocalOutputFile(path));
            this.geoParquet = geoParquet;
            this.schema = schema;
        }

        @Override
        protected AvroGeoParquetBuilder<T> self() {
            return this;
        }

        @Override
        protected WriteSupport<T> getWriteSupport(Configuration configuration) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected WriteSupport<T> getWriteSupport(ParquetConfiguration conf) {
            return new AvroGeoParquetWriterSupport<>(schema, geoParquet, conf, extraMetadata);
        }
    }


    public static class AvroGeoParquetWriterSupport<T> extends AvroWriteSupport<T> {
        private final GeoParquet<T> geoParquet;
        private final Map<String, String> extraMetadata;

        private static GenericData model() {
            var model = SpecificData.get();
            model.addLogicalTypeConversion(
                    new org.apache.avro.data.TimeConversions.TimestampMicrosConversion());
            return model;
        }

        private static MessageType setLogicalGeometryType(MessageType root, Set<String> columns) {
            var fields = root.getFields().stream()
                    .map(type -> setLogicalGeometry(type, columns))
                    .toList();
            return new MessageType(root.getName(), fields);
        }

        private static Type setLogicalGeometry(Type type, Set<String> columns) {
            if (!columns.contains(type.getName()) ||
                !type.isPrimitive() ||
                !type.asPrimitiveType().getPrimitiveTypeName().equals(BINARY)) {
                return type;
            }
            return Types.primitive(BINARY, type.getRepetition()).as(geometryType(null)).named(type.getName());
        }

        private static <T> MessageType messageType(Schema schema, GeoParquet<T> geoParquet, ParquetConfiguration conf) {
            var geometryColumns = new HashSet<String>();
            geoParquet.getColumns().stream()
                    .filter(column -> column.encoding() == GeoParquet.Encoding.WKB)
                    .forEach(column -> geometryColumns.add(column.name()));
            return setLogicalGeometryType(new AvroSchemaConverter(conf).convert(schema), geometryColumns);
        }

        public AvroGeoParquetWriterSupport(Schema schema, GeoParquet<T> geoParquet, ParquetConfiguration conf, Map<String, String> extraMetadata) {
            super(messageType(schema, geoParquet, conf), schema, model());
            this.geoParquet = geoParquet;
            this.extraMetadata = extraMetadata;
        }

        @Override
        public void write(T record) {
            super.write(record);
            geoParquet.update(record);
        }

        @Override
        public FinalizedWriteContext finalizeWrite() {
            var extraMetaData = new HashMap<>(extraMetadata);
            extraMetaData.put("geo", geoParquet.schema());
            return new FinalizedWriteContext(extraMetaData);
        }
    }
}
