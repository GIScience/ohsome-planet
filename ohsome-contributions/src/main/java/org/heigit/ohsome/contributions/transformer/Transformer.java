package org.heigit.ohsome.contributions.transformer;

import com.google.common.collect.Iterables;
import me.tongfei.progressbar.ProgressBar;
import org.apache.avro.specific.SpecificData;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.heigit.ohsome.contributions.avro.Contrib;
import org.heigit.ohsome.contributions.avro.ContribChangeset;
import org.heigit.ohsome.contributions.rocksdb.RocksUtil;
import org.heigit.ohsome.contributions.util.Progress;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMType;
import org.heigit.ohsome.osm.pbf.BlobHeader;
import org.heigit.ohsome.osm.pbf.OSMPbf;
import org.heigit.ohsome.parquet.avro.AvroUtil;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.nio.file.StandardOpenOption.READ;

public abstract class Transformer<E extends OSMEntity, R> {
    protected static final Instant VALID_TO = Instant.parse("2222-01-01T00:00:00Z");
    private static final int BLOCKING_FACTOR = 4;

    protected final OSMType osmType;
    protected final OSMPbf pbf;
    protected final Path outputDir;
    protected final int parallel;

    protected Transformer(OSMType type, OSMPbf pbf, Path out, int parallel) {
        this.osmType = type;
        this.pbf = pbf;
        this.outputDir = out;
        this.parallel = parallel;
    }

    public record Chunk(int start, int limit) {
    }

    public static List<Chunk> blocksPerChunk(List<BlobHeader> blobs, int parallel) {
        var size = blobs.size();
        var chunkLength = size / parallel;
        var rest = size % parallel;
        if (chunkLength == 0) {
            chunkLength = 1;
            rest = 0;
        }

        var chunks = new ArrayList<Chunk>();
        for (var offset = 0; offset < size; ) {
            var limit = offset + chunkLength + (rest-- > 0 ? 1 : 0);
            chunks.add(new Chunk(offset, Math.min(size, limit)));
            offset = limit;
        }
        return chunks;
    }

    protected void process(Map<OSMType, List<BlobHeader>> blobsByType) throws IOException {
        var blobs = blobsByType.get(osmType);
        var chunks = blocksPerChunk(blobs, parallel);
        try (var progress = new ProgressBar("read " + osmType, blobs.size());
             var ch = FileChannel.open(pbf.path(), READ)) {
            Flux.range(0, chunks.size())
                    .flatMap(id -> Mono.fromRunnable(() -> process(id, progress::stepBy, ch, chunks.get(id), blobs))
                            .subscribeOn(Schedulers.boundedElastic()), parallel)
                    .blockLast();
        }
    }

    private void process(int id, Progress progress, FileChannel ch, Chunk chunk, List<BlobHeader> blobs) {
        try {
            var processor = Transformer.<E>processor(id, ch, chunk, blobs, pbf);
            process(processor, progress);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Processor processor(int id, FileChannel ch, Chunk chunk, List<BlobHeader> blobs, OSMPbf pbf) {
        return new Processor(id, pbf, ch, blobs, chunk.start(), chunk.limit());
    }

    private static final String GEO_SCHEMA = """
            {"version":"1.0.0","primary_column":"geometry","columns": {
            "geometry":{"encoding":"WKB","geometry_types":["Point","LineString","Polygon","Multipolygon","GeometryCollection"]}}}
            """.replace("\n", "");

    public static Parquet openWriter(Path outputDir, OSMType type, Consumer<AvroUtil.AvroBuilder<Contrib>> config) {
        return new Parquet(outputDir, type, config);
    }

    public static ParquetWriter<Contrib> openWriter(Path path, Consumer<AvroUtil.AvroBuilder<Contrib>> config) throws IOException {
        var model = SpecificData.get();
        model.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.TimestampMicrosConversion());
        var builder = AvroUtil.<Contrib>openWriter(Contrib.getClassSchema(), path)
                .withDataModel(model)
                .withAdditionalMetadata("geo", GEO_SCHEMA)
                .withCompressionCodec(CompressionCodecName.ZSTD)
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

        config.accept(builder);
        return builder.build();
    }

    public static Function<Long, ContribChangeset> getChangeset(Map<Long, ContribChangeset> changesets) {
        return cs -> changesets.computeIfAbsent(cs, id -> ContribChangeset.newBuilder()
                .setId(id)
                .setCreatedAt(Instant.ofEpochSecond(0)).setClosedAt(Instant.parse("2525-01-01T00:00:00Z"))
                .setTags(Map.of()).setHashtags(List.of()).build());
    }

    protected abstract void process(Processor processor, Progress progress) throws Exception;


    protected Map<Long, ContribChangeset> fetchChangesets(Set<Long> ids) {
        var changesetBuilder = ContribChangeset.newBuilder();
        var changesets = new HashMap<Long, ContribChangeset>();
        for (var id : ids) {
            changesets.put(id, changesetBuilder.setId(id)
                    .setCreatedAt(Instant.ofEpochSecond(0))
                    .setClosedAt(VALID_TO)
                    .setTags(Map.of())
                    .setHashtags(List.of()).build());
        }
        return changesets;
    }

    protected <T extends OSMEntity> boolean hasTags(List<T> osh) {
        return Iterables.any(osh, osm -> !osm.tags().isEmpty());
    }

    public static void moveSstToRocksDb(Path rocksDbPath) throws RocksDBException, IOException {
        try (var options = RocksUtil.defaultOptions().setCreateIfMissing(true);
             var rocksDb = RocksDB.open(options, rocksDbPath.toString())) {
            var ifo = new IngestExternalFileOptions();
            ifo.setMoveFiles(true);
            ifo.setWriteGlobalSeqno(false);
            try (var files = Files.list(rocksDbPath.resolve("ingest"))) {
                rocksDb.ingestExternalFile(files
                        .map(Path::toAbsolutePath)
                        .map(Path::toString).toList(), ifo);
            }
        }
        Files.deleteIfExists(rocksDbPath.resolve("ingest"));
    }

    public static class Parquet implements Closeable {

        private final Path outputDir;
        private final OSMType type;
        private final Consumer<AvroUtil.AvroBuilder<Contrib>> config;

        private final Map<CharSequence, ParquetWriter<Contrib>> writers = new HashMap<>();

        public Parquet(Path outputDir, OSMType type, Consumer<AvroUtil.AvroBuilder<Contrib>> config) {
            this.outputDir = outputDir;
            this.type = type;
            this.config = config;
        }

        @Override
        public void close() throws IOException {
            var suppressed = new ArrayList<IOException>();
            for (var writer : writers.values()) {
                try {
                    writer.close();
                } catch (IOException e) {
                    suppressed.add(e);
                }
            }
            if (!suppressed.isEmpty()) {
                var exceptions = new IOException("error closing parquet writers!");
                suppressed.forEach(exceptions::addSuppressed);
                throw exceptions;
            }

        }

        public void write(long processorId, Contrib contrib) throws IOException {
            var status = "latest".contentEquals(contrib.getStatus()) ? "latest" : "history";
            var writer = writers.get(status);

            if (writer == null) {
                var path = outputDir.resolve("contributions")
                        .resolve(status)
                        .resolve("%s-%d-%d-contribs.parquet".formatted(type, processorId, contrib.getOsmId()));
                Files.createDirectories(path.getParent());
                writer = openWriter(path, config);
                writers.put(status, writer);
            }
            writer.write(contrib);
        }
    }
}
