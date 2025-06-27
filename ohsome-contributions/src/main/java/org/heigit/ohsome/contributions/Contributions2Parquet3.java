package org.heigit.ohsome.contributions;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.atomic.AtomicBoolean;
import me.tongfei.progressbar.ProgressBarBuilder;
import org.heigit.ohsome.contributions.avro.Contrib;
import org.heigit.ohsome.contributions.avro.ContribChangeset;
import org.heigit.ohsome.contributions.contrib.*;
import org.heigit.ohsome.contributions.minor.MinorNode;
import org.heigit.ohsome.contributions.minor.MinorNodeStorage;
import org.heigit.ohsome.contributions.minor.MinorWay;
import org.heigit.ohsome.contributions.rocksdb.RocksUtil;
import org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner;
import org.heigit.ohsome.contributions.transformer.TransformerNodes;
import org.heigit.ohsome.contributions.transformer.TransformerWays;
import org.heigit.ohsome.contributions.util.RocksMap;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMEntity.OSMRelation;
import org.heigit.ohsome.osm.changesets.Changesets;
import org.heigit.ohsome.osm.pbf.Blob;
import org.heigit.ohsome.osm.pbf.BlobHeader;
import org.heigit.ohsome.osm.pbf.Block;
import org.heigit.ohsome.osm.pbf.OSMPbf;
import org.heigit.ohsome.parquet.avro.AvroUtil;
import org.rocksdb.RocksDB;
import picocli.CommandLine;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

import static com.google.common.base.Predicates.alwaysFalse;
import static com.google.common.base.Predicates.alwaysTrue;
import static java.nio.file.StandardOpenOption.READ;
import static org.heigit.ohsome.osm.OSMType.*;
import static org.heigit.ohsome.osm.pbf.OSMPbf.blobBuffer;
import static org.heigit.ohsome.osm.pbf.OSMPbf.blockBuffer;
import static org.heigit.ohsome.osm.pbf.ProtoZero.decodeMessage;
import static reactor.core.publisher.Mono.fromCallable;
import static reactor.core.scheduler.Schedulers.parallel;

@CommandLine.Command(name = "contributions3", aliases = {"contribs3"},
        mixinStandardHelpOptions = true,
        version = "ohsome-planet contribution 1.0.0", //TODO version should be automatically set see picocli.CommandLine.IVersionProvider
        description = "generates parquet files")
public class Contributions2Parquet3 implements Callable<Integer> {

    @CommandLine.Option(names = {"--pbf"}, required = true)
    private Path pbfPath;

    @CommandLine.Option(names = {"--output"})
    private Path output = Path.of("out");

    @CommandLine.Option(names = {"--overwrite"})
    private boolean overwrite = false;

    @CommandLine.Option(names = {"--parallel"})
    private int parallel = Runtime.getRuntime().availableProcessors() - 1;

    @CommandLine.Option(names = {"--country-file"})
    private Path countryFilePath;

    @CommandLine.Option(names = {"--changeset-db"}, description = "full jdbc:url to changesetmd database e.g. jdbc:postgresql://HOST[:PORT]/changesets?user=USER&password=PASSWORD")
    private String changesetDbUrl = "";

    @CommandLine.Option(names = {"--files"}, description = "number of files which will created. Default parallel * 2")
    private int numFiles = 0;

    @CommandLine.Option(names = {"--include-tags"}, description = "OSM keys of relations that should be built")
    private String includeTags = "";

    private Scheduler readerScheduler;
    private SpatialJoiner countryJoiner;
    private Changesets changesetDb;

    @Override
    public Integer call() throws Exception {
        var pbf = OSMPbf.open(pbfPath);

        var totalTime = Stopwatch.createStarted();

        var blobHeaders = getBlobHeaders(pbf);
        var blobTypes = pbf.blobsByType(blobHeaders);

        countryJoiner = Optional.ofNullable(countryFilePath)
                .map(SpatialJoiner::fromCSVGrid)
                .orElseGet(SpatialJoiner::noop);

        changesetDb = Changesets.open(changesetDbUrl, parallel);

        var keyFilter = new HashMap<String, Predicate<String>>();
        if (!includeTags.isBlank()) {
            for (var tag : includeTags.split(",")) {
                keyFilter.put(tag, alwaysTrue());
            }
        }

        if (numFiles == 0) {
            numFiles = parallel * 2;
        }

        Files.createDirectories(output);


        RocksDB.loadLibrary();
        var minorNodesPath = output.resolve("minorNodes");
        TransformerNodes.processNodes(pbf, blobTypes, output, parallel, numFiles, minorNodesPath, countryJoiner, changesetDb);
        var minorWaysPath = output.resolve("minorWays");
        try (var minorNodes = MinorNodeStorage.inRocksMap(minorNodesPath)) {
            TransformerWays.processWays(pbf, blobTypes, output, parallel, numFiles, minorNodes, minorWaysPath, x -> true, countryJoiner, changesetDb);
        }

        try (var ch = FileChannel.open(pbfPath, READ);
             var options = RocksUtil.defaultOptions().setCreateIfMissing(true);
             var minorNodesDb = RocksDB.open(options, output.resolve("minorNodes").toString());
             var minorWaysDb = RocksDB.open(options, output.resolve("minorWays").toString());
             var progress = new ProgressBarBuilder()
                .setTaskName("process %8s".formatted(RELATION))
                .setInitialMax(blobTypes.get(RELATION).size())
                .setUnit(" blk", 1)
                .build()) {

            readerScheduler =
                    Schedulers.newBoundedElastic(10 * Runtime.getRuntime().availableProcessors(), 10_000, "reader", 60, true);

            var writers = new ArrayBlockingQueue<Writer>(numFiles);
            for (var i = 0; i < numFiles; i++) {
                writers.add(new Writer(i, RELATION, output, Contributions2Parquet3::relationParquetConfig));
            }

            var blocks = Flux.fromIterable(blobTypes.get(RELATION))
                    // read blob from file
                    .flatMapSequential(blobHeader -> fromCallable(() -> decodeMessage(blobBuffer(ch, blobHeader), Blob::new))
                            .subscribeOn(readerScheduler), parallel)
                    // decompress blob into block
                    .flatMapSequential(blob -> fromCallable(() -> decodeMessage(blockBuffer(blob), Block::new))
                            .subscribeOn(parallel()), parallel)
                    .toIterable(10).iterator();

            var contribWorkers = Executors.newFixedThreadPool(numFiles, new ThreadFactoryBuilder()
                    .setNameFormat("contrib-worker-%d")
                    .setDaemon(true)
                    .build());

            var entities = Iterators.peekingIterator(new OSMIterator(blocks, progress));
            var osh = new ArrayList<OSMEntity>();
            var cancel = new AtomicBoolean(false);
            while (entities.hasNext()) {
                osh.clear();
                var id = entities.peek().id();
                while (entities.hasNext() && entities.peek().id() == id) {
                    osh.add(entities.next());
                }
                if (entities.hasNext() && entities.peek().id() <= id) {
                    System.out.println("Ids not in order: " + id + " <= " + entities.peek().id());
                }
                if (cancel.get()) {
                    System.out.println("canceled");
                    break;
                }

                var copy = List.copyOf(osh);
                var writer = writers.take();
                contribWorkers.execute(() -> {
                    try {
                        processRelation(id, copy, writer, countryJoiner, changesetDb, minorNodesDb, minorWaysDb);
                    } catch (Exception e) {
                        cancel.set(true);
                        e.printStackTrace();
                    } finally {
                        writers.offer(writer);
                    }
                });
            }
            for (var i = 0; i < numFiles; i++) {
                var writer = writers.take();
                writer.close(cancel.get());
            }
        }

        System.out.println("done " + totalTime);
        return 0;
    }

    private static void relationParquetConfig(AvroUtil.AvroBuilder<Contrib> config) {
        config.withMinRowCountForPageSizeCheck(1)
                .withMaxRowCountForPageSizeCheck(2);
    }

    private static void processRelation(long id, List<OSMEntity> entities, Writer writer, SpatialJoiner spatialJoiner, Changesets changesetDb, RocksDB minorNodesDb, RocksDB minorWaysDb) throws Exception {
        var minorNodeIds = new HashSet<Long>();
        var minorMemberIds = Map.of(
                NODE, minorNodeIds,
                WAY, Sets.<Long>newHashSetWithExpectedSize(64_000));

        var changesetIds = new HashSet<Long>();
        var osh = new ArrayList<OSMRelation>(entities.size());
        entities.forEach(entity -> {
            var osm = (OSMRelation) entity;
            osm.members().stream()
                    .filter(member -> member.type() != RELATION)
                    .forEach(member -> minorMemberIds.get(member.type()).add(member.id()));
            changesetIds.add(osm.changeset());
            osh.add(osm);
        });

        var minorWays = RocksMap.get(minorWaysDb, minorMemberIds.get(WAY), MinorWay::deserialize);
        minorWays.values().stream()
                .<OSMEntity.OSMWay>mapMulti(Iterable::forEach)
                .forEach(osm -> {
                    minorNodeIds.addAll(osm.refs());
                    changesetIds.add(osm.changeset());
                });

        var minorNodes = RocksMap.get(minorNodesDb, minorNodeIds, MinorNode::deserialize);
        minorNodes.values().stream()
                .<OSMEntity.OSMNode>mapMulti(Iterable::forEach)
                .map(OSMEntity.OSMNode::changeset)
                .forEach(changesetIds::add);

        var changesets = fetchChangesets(changesetIds, changesetDb);

        var time = System.nanoTime();
        var contributions = new ContributionsRelation(osh, Contributions.memberOf(minorNodes, minorWays));
        var converter = new ContributionsAvroConverter(contributions, changesets::get, spatialJoiner);
        var versions = 0;
        while (converter.hasNext()) {
            var contrib = converter.next();
            writer.write(contrib);
            versions++;
        }
        writer.log("%d,%d,%d".formatted(id, versions, System.nanoTime() - time));
    }




    protected static Map<Long, ContribChangeset> fetchChangesets(Set<Long> ids, Changesets changesetDb) throws Exception {
        var changesetBuilder = ContribChangeset.newBuilder();
        var changesets = changesetDb.changesets(ids, (id, created, closed, tags, hashtags, editor, numChanges) ->
                changesetBuilder
                        .setId(id)
                        .setCreatedAt(created)
                        .setClosedAt(closed)
                        .setTags(Map.copyOf(tags))
                        .setHashtags(List.copyOf(hashtags))
                        .setEditor(editor)
                        .setNumChanges(numChanges)
                        .build());
        changesetBuilder
                .setCreatedAt(Instant.ofEpochSecond(0))
                .clearClosedAt().clearTags().clearHashtags().clearEditor().clearNumChanges();
        ids.forEach(id -> changesets.computeIfAbsent(id, x -> changesetBuilder.setId(id).build()));
        return changesets;
    }

    protected static <T extends OSMEntity> boolean hasNoTags(List<T> osh) {
        return Iterables.all(osh, osm -> osm.tags().isEmpty());
    }

    protected static <T extends OSMEntity> boolean filter(List<T> osh, Map<String, Predicate<String>> keyFilter) {
        if (keyFilter.isEmpty()) return false;
        return osh.stream()
                .map(OSMEntity::tags)
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .noneMatch(tag -> keyFilter.getOrDefault(tag.getKey(), alwaysFalse()).test(tag.getValue()));
    }

    public static List<BlobHeader> getBlobHeaders(OSMPbf pbf) {
        var blobHeaders = new ArrayList<BlobHeader>();
        try (var progress = new ProgressBarBuilder()
                .setTaskName("read blocks")
                .setInitialMax(pbf.size())
                .setUnit(" MiB", 1L << 20)
                .build()) {
            pbf.blobs().forEach(blobHeader -> {
                progress.stepTo(blobHeader.offset() + blobHeader.dataSize());
                blobHeaders.add(blobHeader);
            });
            progress.setExtraMessage(blobHeaders.size() + " blocks");
        }
        return blobHeaders;
    }

    public static void main(String[] args) {
        var main = new Contributions2Parquet3();
        var exit = new CommandLine(main).execute(args);
        System.exit(exit);
    }

}
