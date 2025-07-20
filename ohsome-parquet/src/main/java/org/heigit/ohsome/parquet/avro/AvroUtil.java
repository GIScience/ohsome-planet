package org.heigit.ohsome.parquet.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class AvroUtil {

    private AvroUtil() {}

    public static <T> AvroBuilder<T> openWriter(Schema schema, Path path) throws IOException {
        Files.createDirectories(path.toAbsolutePath().getParent());
        return new AvroBuilder<T>(outputFile(path))
                .withSchema(schema, null)
                .withConf(new Configuration());
    }

    public static <T> boolean write(ParquetWriter<T> writer, T value) throws IOException {
        writer.write(value);
        return true;
    }

    public static void closeWriter(ParquetWriter<?> writer) {
        try {
            writer.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static OutputFile outputFile(Path path) {
        return new LocalOutputFile(path);
    }

    public static LocalInputFile inputFile(Path path) {
        return new LocalInputFile(path);
    }

    public static class AvroBuilder<T> extends ParquetWriter.Builder<T, AvroBuilder<T>> {
        private Schema schema = null;
        private MessageType messageType = null;
        private GenericData model = null;
        private final Map<String, String> additionalMetadata = new HashMap<>();

        public AvroBuilder(OutputFile file) {
            super(file);
        }

        public AvroBuilder<T> withSchema(Schema schema, MessageType messageType) {
            this.schema = schema;
            this.messageType = messageType;
            return this;
        }

        public AvroBuilder<T> withDataModel(GenericData model) {
            this.model = model;
            return this;
        }


        public AvroBuilder<T> withAdditionalMetadata(String key, String value) {
            this.additionalMetadata.put(key, value);
            return this;
        }

        public AvroBuilder<T> withAdditionalMetadata(Map<String, String> metadata) {
            this.additionalMetadata.putAll(metadata);
            return this;
        }


        @Override
        protected AvroBuilder<T> self() {
            return this;
        }

        @Override
        protected WriteSupport<T> getWriteSupport(Configuration conf) {
            messageType = messageType == null ? new AvroSchemaConverter(conf).convert(schema) : null;
            return new AvroWriteSupport<>(messageType, schema, model) {
                @Override
                public WriteContext init(Configuration configuration) {
                    var wc = super.init(configuration);
                    var extraMetaData = new HashMap<>(wc.getExtraMetaData());
                    extraMetaData.putAll(additionalMetadata);
                    return new WriteContext(wc.getSchema(), extraMetaData);
                }
            };
        }
    }

}
