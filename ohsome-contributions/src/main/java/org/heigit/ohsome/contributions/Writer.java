package org.heigit.ohsome.contributions;

import org.apache.parquet.hadoop.ParquetWriter;
import org.heigit.ohsome.contributions.avro.Contrib;
import org.heigit.ohsome.contributions.transformer.Transformer;
import org.heigit.ohsome.osm.OSMType;
import org.heigit.ohsome.parquet.avro.AvroUtil;
import org.heigit.ohsome.util.io.Output;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

class Writer implements AutoCloseable {

    private final Map<String, ParquetWriter<Contrib>> writers = new HashMap<>();

    private final int writerId;
    private final OSMType type;
    private final Path outputDir;
    private final Consumer<AvroUtil.AvroBuilder<Contrib>> config;

    final Output output = new Output(4 << 10);
    final ByteBuffer keyBuffer = ByteBuffer.allocateDirect(Long.BYTES).order(ByteOrder.BIG_ENDIAN);
    ByteBuffer valBuffer = ByteBuffer.allocateDirect(4 << 10);

    Writer(int writerId, OSMType type, Path outputDir, Consumer<AvroUtil.AvroBuilder<Contrib>> config) {
        this.writerId = writerId;
        this.type = type;
        this.outputDir = outputDir;
        this.config = config;
    }

    public void write(Contrib contrib) throws IOException {
        var status = "latest".contentEquals(contrib.getStatus()) ? "latest" : "history";
        writers.computeIfAbsent(status, this::openWriter).write(contrib);
    }

    private ParquetWriter<Contrib> openWriter(String status) {
        var path = progressPath(status);
        try {
            Files.createDirectories(path.getParent());
            return Transformer.openWriter(path, config);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Path progressPath(String status) {
        return outputDir.resolve("progress")
                .resolve("%s-%d-%s-contribs.parquet".formatted(type, writerId, status));
    }

    private Path finalPath(String status) {
        return outputDir.resolve("contributions")
                .resolve(status)
                .resolve("%s-%d-%s-contribs.parquet".formatted(type, writerId, status));
    }

    @Override
    public void close() {
        var suppressed = new ArrayList<IOException>();
        writers.forEach((key, writer) -> {
            try {
                writer.close();
                var path = progressPath(key);
                var finalPath = finalPath(key);
                Files.createDirectories(finalPath.toAbsolutePath().getParent());
                Files.move(path, finalPath);
            } catch (IOException e) {
                suppressed.add(e);
            }
        });
        /*
        if (!suppressed.isEmpty()) {
            var exceptions = new IOException("error closing parquet writers!");
            suppressed.forEach(exceptions::addSuppressed);
            throw new UncheckedIOException(exceptions);
        }
        */
    }

    public ByteBuffer keyBuffer(long id) {
        return keyBuffer.clear().putLong(id).flip();
    }

    public ByteBuffer valBuffer(int length) {
        if (valBuffer.capacity() < length) {
            valBuffer = ByteBuffer.allocateDirect(length);
        }
        return valBuffer.clear();
    }

    public Output output() {
        output.reset();
        return output;
    }
}
