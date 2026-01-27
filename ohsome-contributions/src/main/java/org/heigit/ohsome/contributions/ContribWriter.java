package org.heigit.ohsome.contributions;

import java.util.function.Function;

import org.apache.parquet.hadoop.ParquetWriter;
import org.heigit.ohsome.contributions.avro.Contrib;
import org.heigit.ohsome.osm.OSMType;
import org.heigit.ohsome.output.OutputLocation;
import org.heigit.ohsome.parquet.AvroGeoParquetWriter.AvroGeoParquetBuilder;
import org.heigit.ohsome.util.io.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class ContribWriter implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ContribWriter.class);

    private final Map<String, ParquetWriter<Contrib>> writers = new HashMap<>();

    private final int writerId;
    private final OSMType type;
    private final Path temp;
    private final OutputLocation outputDir;
    private final Consumer<AvroGeoParquetBuilder<Contrib>> additionalConfig;

    final Output output = new Output(4 << 10);

    public ContribWriter(int writerId, OSMType type, Path temp, OutputLocation outputDir, Consumer<AvroGeoParquetBuilder<Contrib>> config) {
        this.writerId = writerId;
        this.type = type;
        this.temp = temp;
        this.outputDir = outputDir;
        this.additionalConfig = config;
    }

    public void write(Contrib contrib) throws IOException {
        var status = "latest".contentEquals(contrib.getStatus()) ? "latest" : "history";
        writers.computeIfAbsent(status, this::openWriter).write(contrib);
    }

    private ParquetWriter<Contrib> openWriter(String status) {
        var path = progressPath(status);
        return ContribUtil.openWriter(path, additionalConfig);
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
