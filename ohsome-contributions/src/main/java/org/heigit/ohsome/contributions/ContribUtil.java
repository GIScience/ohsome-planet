package org.heigit.ohsome.contributions;

import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.heigit.ohsome.contributions.avro.Contrib;
import org.heigit.ohsome.parquet.AvroGeoParquetWriter;
import org.heigit.ohsome.parquet.GeoParquet;
import org.heigit.ohsome.parquet.GeoParquet.Encoding;
import org.heigit.ohsome.parquet.GeoParquet.GeometryType;
import org.locationtech.jts.geom.Envelope;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

import static java.util.EnumSet.allOf;

public class ContribUtil {

    private ContribUtil() {
        // utility class
    }

    public static final GeoParquet<Contrib> GEO_PARQUET = GeoParquet.<Contrib>builder()
            .column("geometry", Encoding.WKB, allOf(GeometryType.class), "bbox", ContribUtil::geometryEnv)
//            .column("centroid", Encoding.Point, EnumSet.of(GeometryType.Point), ContribUtil::centroidEnv)
            .build("geometry");


    public static ParquetWriter<Contrib> openWriter(Path path, Consumer<AvroGeoParquetWriter.AvroGeoParquetBuilder<Contrib>> configuration) {
        try {
            Files.createDirectories(path.getParent());
            return AvroGeoParquetWriter.openWriter(path, Contrib.getClassSchema(), GEO_PARQUET, config -> configuration.accept(defaultConfiguration(config)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static AvroGeoParquetWriter.AvroGeoParquetBuilder<Contrib> defaultConfiguration(AvroGeoParquetWriter.AvroGeoParquetBuilder<Contrib> config) {
        config.withCompressionCodec(CompressionCodecName.ZSTD)
              .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
              .config(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false")

              .withRowGroupSize(32L * 1024 * 1024).withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
//                .withDictionaryPageSize(4 * ParquetWriter.DEFAULT_PAGE_SIZE)

              .withDictionaryEncoding("osm_id", false);
        return config;
    }


    private static Envelope geometryEnv(Contrib record) {
        var bbox = record.getBbox();
        if (bbox == null) {
            return new Envelope();
        }
        return new Envelope(bbox.getXmin(), bbox.getYmin(), bbox.getXmax(), bbox.getYmax());
    }

    private static Envelope centroidEnv(Contrib record) {
            var centroid = record.getCentroid();
            var env = new Envelope();
            if (centroid == null) {
                return env;
            }
            env.expandToInclude(centroid.getX(), centroid.getY());
            return env;
    }

}
