package org.heigit.ohsome.contributions;

import java.util.EnumSet;
import java.util.function.Function;

import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.heigit.ohsome.contributions.avro.Contrib;
import org.heigit.ohsome.osm.OSMType;
import org.heigit.ohsome.output.OutputLocation;
import org.heigit.ohsome.parquet.AvroGeoParquetWriter;
import org.heigit.ohsome.parquet.AvroGeoParquetWriter.AvroGeoParquetBuilder;
import org.heigit.ohsome.parquet.GeoParquet;
import org.heigit.ohsome.util.io.Output;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class ContribWriter implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ContribWriter.class);

    private static final GeoParquet.GeoParquetBuilder<Contrib> GEO_PARQUET = GeoParquet.<Contrib>builder("geometry")
//            .column("centroid", GeoParquet.Encoding.Point, EnumSet.of(GeoParquet.GeometryType.Point), record -> {
//                var centroid = record.getCentroid();
//                var env = new Envelope();
//                if (centroid == null) {
//                    return env;
//                }
//                env.expandToInclude(centroid.getX(), centroid.getY());
//                return env;
//            })
            .column("geometry", GeoParquet.Encoding.WKB, EnumSet.allOf(GeoParquet.GeometryType.class), "bbox", record -> {
                var bbox = record.getBbox();
                if (bbox == null) {
                    return new Envelope();
                }
                return new Envelope(bbox.getXmin(), bbox.getYmin(), bbox.getXmax(), bbox.getYmax());
            })
            ;

    private final Map<String, ParquetWriter<Contrib>> writers = new HashMap<>();

    private final int writerId;
    private final OSMType type;
    private final Path temp;
    private final OutputLocation outputDir;
    private final Consumer<AvroGeoParquetBuilder<Contrib>> additinalConfig;

    final Output output = new Output(4 << 10);

    public ContribWriter(int writerId, OSMType type, Path temp, OutputLocation outputDir, Consumer<AvroGeoParquetBuilder<Contrib>> config) {
        this.writerId = writerId;
        this.type = type;
        this.temp = temp;
        this.outputDir = outputDir;
        this.additinalConfig = config;
    }

    public void write(Contrib contrib) throws IOException {
        var status = "latest".contentEquals(contrib.getStatus()) ? "latest" : "history";
        writers.computeIfAbsent(status, this::openWriter).write(contrib);
    }

    private ParquetWriter<Contrib> openWriter(String status) {
        var path = progressPath(status);
        try {
            Files.createDirectories(path.getParent());
            return AvroGeoParquetWriter.openWriter(path, Contrib.getClassSchema(), GEO_PARQUET.build(), this::writerConfig);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writerConfig(AvroGeoParquetBuilder<Contrib> config) {
        config.withCompressionCodec(CompressionCodecName.ZSTD)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .config(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false")

                .withRowGroupSize(32L * 1024 * 1024)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
//                .withDictionaryPageSize(4 * ParquetWriter.DEFAULT_PAGE_SIZE)

                .withDictionaryEncoding("osm_id", false)
                .withDictionaryEncoding("refs.list.element", false)
                .withBloomFilterEnabled("refs.list.element", true)

                .withBloomFilterEnabled("user.id", true)

                .withBloomFilterEnabled("changeset.id", true)

                .withDictionaryEncoding("members.list.element.id", false)
                .withBloomFilterEnabled("members.list.element.id", true);
        additinalConfig.accept(config);
    }

    private Path progressPath(String status) {
        return temp.resolve("progress")
                .resolve("%s-%d-%s-contribs.parquet".formatted(type, writerId, status));
    }

    private Path finalPath(String status) {
        return outputDir
                .resolve(status)
                .resolve("%s-%d-%s-contribs.parquet".formatted(type, writerId, status));
    }

    private Path canceledPath(String status) {
        return outputDir.resolve("canceled")
            .resolve(status)
            .resolve("%s-%d-%s-contribs-canceled.parquet".formatted(type, writerId, status));
    }

    @Override
    public void close() {
        close(this::finalPath);
    }

    public Output output() {
        output.reset();
        return output;
    }

    public int getId() {
        return writerId;
    }

    public void close(boolean canceled) {
        if (!canceled) {
            close();
        } else {
            close(this::canceledPath);
        }
    }

    private void close(Function<String, Path> pathFnt) {
        writers.forEach((key, writer) -> {
            logger.debug("closing writer {} {}", getId(), key);
            var path = progressPath(key);
            var finalPath = pathFnt.apply(key);
            try {
                writer.close();

                outputDir.move(path, finalPath);
            } catch (Exception e) {
                logger.error("closing writer {}: {} - {}", getId(), key, finalPath, e);
                // ignore exception
            }
        });
    }
}
