package org.heigit.ohsome.contributions.transformer;

import me.tongfei.progressbar.ProgressBarBuilder;
import org.heigit.ohsome.contributions.ContribWriter;
import org.heigit.ohsome.contributions.avro.Contrib;
import org.heigit.ohsome.contributions.minor.SstWriter;
import org.heigit.ohsome.contributions.rocksdb.RocksUtil;
import org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner;
import org.heigit.ohsome.contributions.util.Progress;
import org.heigit.ohsome.osm.OSMType;
import org.heigit.ohsome.osm.changesets.Changesets;
import org.heigit.ohsome.osm.pbf.BlobHeader;
import org.heigit.ohsome.osm.pbf.OSMPbf;
import org.heigit.ohsome.output.OutputLocation;
import org.heigit.ohsome.parquet.AvroGeoParquetWriter.AvroGeoParquetBuilder;
import org.heigit.ohsome.planet.utils.VersionProvider;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.nio.file.StandardOpenOption.READ;

public abstract class Transformer {

    protected final OSMType osmType;
    protected final OSMPbf pbf;
    protected final Path tempDir;
    protected final OutputLocation outputDir;
    protected final int parallel;
    protected final SpatialJoiner countryJoiner;
    protected final Changesets changesetDb;
    protected final Path minorSstDirectory;
    private final Path replicationSstDirectory;

    protected Transformer(OSMType type, OSMPbf pbf, Path temp, OutputLocation out, int parallel,
                          SpatialJoiner countryJoiner, Changesets changesetDb, Path minorSstDir, Path replicationSstDir) {
        this.osmType = type;
        this.pbf = pbf;
        this.tempDir = temp;
        this.outputDir = out;
        this.parallel = parallel;
        this.countryJoiner = countryJoiner;
        this.changesetDb = changesetDb;
        this.minorSstDirectory = minorSstDir.resolve("ingest");
        this.replicationSstDirectory = replicationSstDir != null ? replicationSstDir.resolve("ingest") : null;
    }

    public record Summary(Instant replicationTimestamp, long replicationElementsCount) {

        public static final Summary EMPTY = new Summary(Instant.EPOCH, 0L);
        public static Summary empty() {
            return EMPTY;
        }

        public static Summary merge(Summary a, Summary b) {
            return new Summary(Instant.ofEpochSecond(
                    Math.max(a.replicationTimestamp().getEpochSecond(),
                             b.replicationTimestamp().getEpochSecond())),
                    a.replicationElementsCount() + b.replicationElementsCount());
        }

    }

    protected abstract Summary process(Processor processor, Progress progress, ContribWriter writer, SstWriter sstWriter, SstWriter sstWriterReplication) throws Exception;

    public record Chunk(int start, int limit) {

    }

    public static List<Chunk> blocksPerChunk(List<BlobHeader> blobs, int numChunks) {
        var size = blobs.size();
        var splits = Math.min(size, numChunks);
        var chunkLength = size / splits;
        var rest = size % splits;
        if (chunkLength == 0) {
            chunkLength = 1;
            rest = 0;
        }

        var chunks = new ArrayList<Chunk>();
        var offset = 0;
        while (offset < size) {
            var limit = offset + chunkLength + (rest-- > 0 ? 1 : 0);
            chunks.add(new Chunk(offset, Math.min(size, limit)));
            offset = limit;
        }
        return chunks;
    }

    protected Summary process(Map<OSMType, List<BlobHeader>> blobsByType) throws IOException {
        var blobs = blobsByType.get(osmType);
        var chunks = blocksPerChunk(blobs, parallel);
        try (var progress = new ProgressBarBuilder()
                .setTaskName("process %8s".formatted(osmType))
                .setInitialMax(blobs.size())
                .setUnit(" blk", 1)
                .build();
             var ch = FileChannel.open(pbf.path(), READ)) {
            return Flux.range(0, chunks.size())
                    .flatMap(id -> Mono.fromCallable(
                                    () -> process(id, progress::stepBy, ch, chunks.get(id), blobs))
                            .subscribeOn(Schedulers.boundedElastic()), parallel)
                    .reduce(Summary::merge)
                    .blockOptional().orElseThrow();
        }
    }

    private Summary process(int id, Progress progress, FileChannel ch, Chunk chunk,
                         List<BlobHeader> blobs) {
        try {
            var processor = Transformer.processor(id, ch, chunk, blobs, pbf);
            return process(processor, progress);
        } catch (Exception e) {
            throw new TransformerException("Error processing chunk " + id, e);
        }
    }

    protected void avroConfig(AvroGeoParquetBuilder<Contrib> config) {
    }


    public static Processor processor(int id, FileChannel ch, Chunk chunk, List<BlobHeader> blobs,
                                      OSMPbf pbf) {
        return new Processor(id, pbf, ch, blobs, chunk.start(), chunk.limit());
    }

    public static ContribWriter openWriter(int writerId, Path tempDir, OutputLocation outputDir, OSMType type,
                                           Consumer<AvroGeoParquetBuilder<Contrib>> config) {
        return new ContribWriter(writerId, type, tempDir, outputDir, config);
    }

    protected Summary process(Processor processor, Progress progress) throws Exception {
        try (var writer = openWriter(processor.id(), tempDir, outputDir, osmType, this::avroConfig)) {
            return process(processor, progress, writer);
        }
    }

    protected Summary process(Processor processor, Progress progress, ContribWriter writer) throws Exception {
        var minorSSTPath = minorSstDirectory.resolve("%03d.sst".formatted(processor.id()));
        try ( var option = RocksUtil.defaultOptions(true);
              var minorSSTWriter = new SstWriter(minorSSTPath, option);
              var replicationSSTWriter =  replicationSstDirectory != null ? new SstWriter(replicationSstDirectory.resolve("%03d.sst".formatted(processor.id())), option): null) {
            return process(processor, progress, writer, minorSSTWriter, replicationSSTWriter);
        }
    }


    public static void moveSstToRocksDb(Path rocksDbPath) throws RocksDBException, IOException {
        if(rocksDbPath == null) {
            return;
        }
        try (var options = RocksUtil.defaultOptions().setCreateIfMissing(true);
             var rocksDb = RocksDB.open(options, rocksDbPath.toString());
             var ifo = new IngestExternalFileOptions()) {
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


}
