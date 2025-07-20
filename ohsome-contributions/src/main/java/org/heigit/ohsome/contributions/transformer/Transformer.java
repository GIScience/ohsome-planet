package org.heigit.ohsome.contributions.transformer;

import me.tongfei.progressbar.ProgressBarBuilder;
import org.apache.avro.specific.SpecificData;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.heigit.ohsome.contributions.avro.Contrib;
import org.heigit.ohsome.contributions.rocksdb.RocksUtil;
import org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner;
import org.heigit.ohsome.contributions.util.Progress;
import org.heigit.ohsome.osm.OSMType;
import org.heigit.ohsome.osm.changesets.Changesets;
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
import java.util.*;
import java.util.function.Consumer;

import static java.nio.file.StandardOpenOption.READ;

public abstract class Transformer {

  protected final OSMType osmType;
  protected final OSMPbf pbf;
  protected final Path outputDir;
  protected final int parallel;
  protected final SpatialJoiner countryJoiner;
  protected final Changesets changesetDb;

  protected Transformer(OSMType type, OSMPbf pbf, Path out, int parallel,
      SpatialJoiner countryJoiner, Changesets changesetDb) {
    this.osmType = type;
    this.pbf = pbf;
    this.outputDir = out;
    this.parallel = parallel;
    this.countryJoiner = countryJoiner;
    this.changesetDb = changesetDb;
  }

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
    while(offset < size) {
      var limit = offset + chunkLength + (rest-- > 0 ? 1 : 0);
      chunks.add(new Chunk(offset, Math.min(size, limit)));
      offset = limit;
    }
    return chunks;
  }

  protected void process(Map<OSMType, List<BlobHeader>> blobsByType) throws IOException {
    var blobs = blobsByType.get(osmType);
    var chunks = blocksPerChunk(blobs, parallel);
    try (var progress = new ProgressBarBuilder()
        .setTaskName("process %8s".formatted(osmType))
        .setInitialMax(blobs.size())
        .setUnit(" blk", 1)
        .build();
        var ch = FileChannel.open(pbf.path(), READ)) {
      Flux.range(0, chunks.size())
          .flatMap(id -> Mono.fromRunnable(
                  () -> process(id, progress::stepBy, ch, chunks.get(id), blobs))
              .subscribeOn(Schedulers.boundedElastic()), parallel)
          .blockLast();
    }
  }

  private void process(int id, Progress progress, FileChannel ch, Chunk chunk,
      List<BlobHeader> blobs) {
    try {
      var processor = Transformer.processor(id, ch, chunk, blobs, pbf);
      process(processor, progress);
    } catch (Exception e) {
      throw new TransformerException("Error processing chunk " + id, e);
    }
  }

  public static Processor processor(int id, FileChannel ch, Chunk chunk, List<BlobHeader> blobs,
      OSMPbf pbf) {
    return new Processor(id, pbf, ch, blobs, chunk.start(), chunk.limit());
  }

  private static final String GEO_SCHEMA = """
      {"version":"1.0.0","primary_column":"geometry","columns": {
      "geometry":{"encoding":"WKB","geometry_types":["Point","LineString","Polygon","Multipolygon","GeometryCollection"]}}}
      """.replace("\n", "");

  public static Parquet openWriter(Path outputDir, OSMType type,
      Consumer<AvroUtil.AvroBuilder<Contrib>> config) {
    return new Parquet(outputDir, type, config);
  }

  public static ParquetWriter<Contrib> openWriter(Path path,
      Consumer<AvroUtil.AvroBuilder<Contrib>> config) throws IOException {
    var model = SpecificData.get();
    model.addLogicalTypeConversion(
        new org.apache.avro.data.TimeConversions.TimestampMicrosConversion());
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

  protected abstract void process(Processor processor, Progress progress) throws Exception;

  public static void moveSstToRocksDb(Path rocksDbPath) throws RocksDBException, IOException {
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

  public static class Parquet implements Closeable {

    record WriterPath(ParquetWriter<Contrib> writer, Path path) {

    }

    private final Path outputDir;
    private final OSMType type;
    private final Consumer<AvroUtil.AvroBuilder<Contrib>> config;

    private final Map<String, WriterPath> writers = new HashMap<>();

    public Parquet(Path outputDir, OSMType type, Consumer<AvroUtil.AvroBuilder<Contrib>> config) {
      this.outputDir = outputDir;
      this.type = type;
      this.config = config;
    }

    @Override
    public void close() throws IOException {
      var suppressed = new ArrayList<IOException>();
      for (var entry : writers.entrySet()) {
        var status = entry.getKey();
        var writerPath = entry.getValue();
        try {
          writerPath.writer().close();
          var newPath = outputDir.resolve("contributions")
              .resolve(status)
              .resolve(writerPath.path().getFileName());
          Files.createDirectories(newPath.toAbsolutePath().getParent());
          Files.move(writerPath.path(), newPath);
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
      var writerPath = writers.get(status);

      if (writerPath == null) {
        var path = outputDir.resolve("progress")
            .resolve("%s-%d-%d-%s-contribs.parquet".formatted(type, processorId, contrib.getOsmId(),
                status));
        Files.createDirectories(path.getParent());
        writerPath = new WriterPath(openWriter(path, config), path);
        writers.put(status, writerPath);
      }
      writerPath.writer().write(contrib);
    }
  }
}
