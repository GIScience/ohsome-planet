package org.heigit.ohsome.contributions;

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
import java.util.function.Consumer;

public class ContribWriter implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ContribWriter.class);

    private final int writerId;
    private final OSMType type;
    private final Path temp;
    private final OutputLocation outputDir;
    private final Consumer<AvroGeoParquetBuilder<Contrib>> additionalConfig;

    private ParquetWriter<Contrib> writer;

    final Output output = new Output(4 << 10);

    public ContribWriter(int writerId, OSMType type, Path temp, OutputLocation outputDir, Consumer<AvroGeoParquetBuilder<Contrib>> config) {
        this.writerId = writerId;
        this.type = type;
        this.temp = temp;
        this.outputDir = outputDir;
        this.additionalConfig = config;
    }

    public void write(Contrib contrib) throws IOException {
        if (writer == null) {
            writer = openWriter();
        }
        writer.write(contrib);
    }

    private ParquetWriter<Contrib> openWriter() {
        return ContribUtil.openWriter(progressPath(), additionalConfig);
    }

    private Path progressPath() {
        return temp.resolve("progress")
                .resolve("%s-%d-contribs.parquet".formatted(type, writerId));
    }

    private Path finalPath() {
        return outputDir
                .resolve("%s-%d-contribs.parquet".formatted(type, writerId));
    }

    private Path canceledPath() {
        return outputDir.resolve("canceled")
                .resolve("%s-%d-contribs-canceled.parquet".formatted(type, writerId));
    }

    @Override
    public void close() {
        close(finalPath());
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
            close(canceledPath());
        }
    }

    private void close(Path finalPath) {
        if (writer == null){
            return;
        }

        var path = progressPath();
        logger.debug("closing writer {}", getId());
        try {
            writer.close();
            outputDir.move(path, finalPath);
        } catch(Exception e){
            logger.error("closing writer {}: {}", getId(), finalPath, e);
            // ignore exception
        }
    }
}
