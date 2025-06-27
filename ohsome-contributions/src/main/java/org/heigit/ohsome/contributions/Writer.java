package org.heigit.ohsome.contributions;

import java.util.function.Function;
import org.apache.parquet.hadoop.ParquetWriter;
import org.heigit.ohsome.contributions.avro.Contrib;
import org.heigit.ohsome.contributions.transformer.Transformer;
import org.heigit.ohsome.osm.OSMType;
import org.heigit.ohsome.parquet.avro.AvroUtil;
import org.heigit.ohsome.util.io.Output;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
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
    private PrintWriter logWriter;

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

    public void log(String message) {
        if (logWriter == null) {
            logWriter = openLogWriter();

        }
        logWriter.println(message);
    }

    private PrintWriter openLogWriter() {
        var path = logPath();
        try {
            Files.createDirectories(path.getParent());
            return new PrintWriter(Files.newOutputStream(path), true);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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

    private Path logPath() {
        return outputDir.resolve("log")
                .resolve("writer-%s-%d.log".formatted(type, writerId));
    }

    private Path finalPath(String status) {
        return outputDir.resolve("contributions")
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
            try {
                writer.close();
                var path = progressPath(key);
                var finalPath = pathFnt.apply(key);
                Files.createDirectories(finalPath.toAbsolutePath().getParent());
                Files.move(path, finalPath);
            } catch (IOException e) {
            }
        });
        if (logWriter != null) {
            logWriter.close();
        }
    }
}
