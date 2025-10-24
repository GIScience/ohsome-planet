package org.heigit.ohsome.parquet;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.heigit.ohsome.parquet.avro.AvroUtil;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Consumer;

public class ParquetUtil {

    private static final String GEO_SCHEMA = """
      {"version":"1.0.0","primary_column":"geometry","columns": {
      "geometry":{"encoding":"WKB","geometry_types":["Point","LineString","Polygon","Multipolygon","GeometryCollection"]}}}
      """.replace("\n", "");

    public static <T> ParquetWriter<T> openWriter(Path path, Schema schema, Consumer<AvroUtil.AvroBuilder<T>> config) throws IOException {
        var model = SpecificData.get();
        model.addLogicalTypeConversion(
                new org.apache.avro.data.TimeConversions.TimestampMicrosConversion());
        var builder = AvroUtil.<T>openWriter(schema, path)
                .withDataModel(model)
                .withAdditionalMetadata("geo", GEO_SCHEMA)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .config(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false")

                .withRowGroupSize(32L * 1024 * 1024)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE);
        config.accept(builder);
        return builder.build();
    }
}
