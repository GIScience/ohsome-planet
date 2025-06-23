package org.heigit.ohsome.contributions;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
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
import org.heigit.ohsome.osm.OSMType;
import org.heigit.ohsome.osm.changesets.Changesets;
import org.heigit.ohsome.osm.pbf.Blob;
import org.heigit.ohsome.osm.pbf.BlobHeader;
import org.heigit.ohsome.osm.pbf.Block;
import org.heigit.ohsome.osm.pbf.OSMPbf;
import org.heigit.ohsome.parquet.avro.AvroUtil;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.google.common.base.Predicates.alwaysFalse;
import static com.google.common.base.Predicates.alwaysTrue;
import static java.nio.file.StandardOpenOption.READ;
import static org.heigit.ohsome.osm.OSMType.*;
import static org.heigit.ohsome.osm.pbf.OSMPbf.blobBuffer;
import static org.heigit.ohsome.osm.pbf.OSMPbf.blockBuffer;
import static org.heigit.ohsome.osm.pbf.ProtoZero.decodeMessage;
import static reactor.core.publisher.Mono.fromCallable;
import static reactor.core.scheduler.Schedulers.boundedElastic;
import static reactor.core.scheduler.Schedulers.parallel;

@CommandLine.Command(name = "contributions2", aliases = {"contribs2"},
        mixinStandardHelpOptions = true,
        version = "ohsome-planet contribution 1.0.0", //TODO version should be automatically set see picocli.CommandLine.IVersionProvider
        description = "generates parquet files")
public class Contributions2Parquet2 implements Callable<Integer> {

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
             var minorWaysDb = RocksDB.open(options, output.resolve("minorWays").toString())) {

            readerScheduler =
                    Schedulers.newBoundedElastic(10 * Runtime.getRuntime().availableProcessors(), 10_000, "reader", 60, true);

//            process(ch, NODE, blobTypes.get(NODE), 10_000, (osh, writers) -> processNodes(osh, writers, countryJoiner, changesetDb, minorNodesDb));
//            process(ch, WAY, blobTypes.get(WAY), 10_000, (osh, writers) -> processWays(osh, writers, countryJoiner, changesetDb, minorNodesDb, minorWaysDb));
            process(ch, RELATION, blobTypes.get(RELATION), 1,
                    (osh, writers) -> processRelations(osh, writers, countryJoiner, changesetDb, keyFilter, minorNodesDb, minorWaysDb),
                    Contributions2Parquet2::relationParquetConfig);
        }

        System.out.println("done " + totalTime);
        return 0;
    }

    interface ProcessFunction {
        long apply(List<List<List<OSMEntity>>> batch, BlockingQueue<Writer> writers) throws Exception;
    }

    private static void relationParquetConfig(AvroUtil.AvroBuilder<Contrib> config) {
        config.withMinRowCountForPageSizeCheck(1)
                .withMaxRowCountForPageSizeCheck(2);
    }


    private long process(FileChannel ch, OSMType type, List<BlobHeader> blobHeaders, int batchSize, ProcessFunction process, Consumer<AvroUtil.AvroBuilder<Contrib>> additionalConfig) {
        var writers = new ArrayBlockingQueue<Writer>(numFiles);
        for (var i = 0; i < numFiles; i++) {
            writers.add(new Writer(i, type, output, additionalConfig));
        }

        long count;
        try (var progress = new ProgressBarBuilder()
                .setTaskName("process %8s".formatted(type))
                .setInitialMax(blobHeaders.size())
                .setUnit(" blk", 1)
                .build()) {

            var oshs = Flux.fromIterable(blobHeaders)
                    // read blob from file
                    .flatMapSequential(blobHeader -> fromCallable(() -> decodeMessage(blobBuffer(ch, blobHeader), Blob::new)).subscribeOn(readerScheduler))
                    // decompress blob into block
                    .flatMapSequential(blob -> fromCallable(() -> decodeMessage(blockBuffer(blob), Block::new))
                            .flatMapMany(block -> Flux.fromStream(block.entities())).bufferUntilChanged(OSMEntity::id).doOnComplete(progress::step).subscribeOn(parallel()))
                    .toStream();
            count = Flux.fromStream(oshs)
                    // overhanging osm_ids/versions only necessary with history files
                    .bufferUntilChanged(list -> list.getFirst().id())
                    .buffer(batchSize)
                    .flatMap(osh -> fromCallable(() -> process.apply(osh, writers)).subscribeOn(boundedElastic()), numFiles)
                    .reduce(Long::sum)
                    .blockOptional().orElseThrow();
        }

        writers.forEach(Writer::close);

        return count;
    }

    private static long processNodes(List<List<List<OSMEntity>>> batch, BlockingQueue<Writer> writers, SpatialJoiner spatialJoiner, Changesets changesetDb, RocksDB minorNodes) throws Exception {
        var writer = Optional.ofNullable(writers.poll()).orElseThrow();
        var counter = 0L;

        var changesetsIds = new HashSet<Long>();

        batch.stream()
                .<List<OSMEntity>>mapMulti(Iterable::forEach)
                .<OSMEntity>mapMulti(Iterable::forEach)
                .forEach(osm -> changesetsIds.add(osm.changeset()));
        var changesets = fetchChangesets(changesetsIds, changesetDb);

        var osh = new ArrayList<OSMEntity.OSMNode>();

        try (var writeBatch = new WriteBatch();
             var writeOpts = new WriteOptions()) {

            for (var entities : batch) {
                osh.clear();
                entities.forEach(list -> list.forEach(osm -> osh.add((OSMEntity.OSMNode) osm)));
                var id = osh.getFirst().id();

                var minorNode = MinorNode.newBuilder();
                osh.forEach(minorNode::add);

                var output = writer.output();
                minorNode.serialize(output);

                var keyBuffer = writer.keyBuffer(id);
                var valBuffer = writer.valBuffer(output.length);

                valBuffer.put(output.array, 0, output.length).flip();
                writeBatch.put(keyBuffer, valBuffer);

                if (hasNoTags(osh)) {
                    continue;
                }

                var contributions = new ContributionsNode(osh);
                var converter = new ContributionsAvroConverter(contributions, changesets::get, spatialJoiner);

                while (converter.hasNext()) {
                    var contrib = converter.next();
                    writer.write(contrib);
                    counter++;
                }
            }

            minorNodes.write(writeOpts, writeBatch);
        }

        writers.add(writer);
        return counter;
    }

    private static long processWays(List<List<List<OSMEntity>>> batch, BlockingQueue<Writer> writers, SpatialJoiner spatialJoiner, Changesets changesetDb, RocksDB minorNodesDb, RocksDB minors) throws Exception {
        var writer = Optional.ofNullable(writers.poll()).orElseThrow();
        var counter = 0L;

        var refs = new HashSet<Long>();
        var changesetIds = new HashSet<Long>();

        batch.stream()
                .<List<OSMEntity>>mapMulti(Iterable::forEach)
                .<OSMEntity>mapMulti(Iterable::forEach)
                .map(OSMEntity.OSMWay.class::cast)
                .forEach(osm -> {
                    refs.addAll(osm.refs());
                    changesetIds.add(osm.changeset());
                });


        var minorNodes = RocksMap.get(minorNodesDb, refs, MinorNode::deserialize);
        minorNodes.values().stream()
                .<OSMEntity.OSMNode>mapMulti(Iterable::forEach)
                .map(OSMEntity.OSMNode::changeset)
                .forEach(changesetIds::add);

        var changesets = fetchChangesets(changesetIds, changesetDb);

        try (var writeBatch = new WriteBatch();
             var writeOpts = new WriteOptions()) {

            var osh = new ArrayList<OSMEntity.OSMWay>();
            for (var entities : batch) {
                osh.clear();
                entities.forEach(list -> list.forEach(osm -> osh.add((OSMEntity.OSMWay) osm)));
                var id = osh.getFirst().id();

                var minor = MinorWay.newBuilder();
                osh.forEach(minor::add);

                var output = writer.output();
                minor.serialize(output);

                var keyBuffer = writer.keyBuffer(id);
                var valBuffer = writer.valBuffer(output.length);

                valBuffer.put(output.array, 0, output.length).flip();
                writeBatch.put(keyBuffer, valBuffer);

                if (hasNoTags(osh)) {
                    continue;
                }

                var contributions = new ContributionsWay(osh, minorNodes);
                var converter = new ContributionsAvroConverter(contributions, changesets::get, spatialJoiner);
                while (converter.hasNext()) {
                    var contrib = converter.next();
                    writer.write(contrib);
                    counter++;
                }
            }
            minors.write(writeOpts, writeBatch);
        }
        writers.add(writer);
        return counter;
    }

    private static long processRelations(List<List<List<OSMEntity>>> batch, BlockingQueue<Writer> writers, SpatialJoiner spatialJoiner, Changesets changesetDb, Map<String, Predicate<String>> keyFilter, RocksDB minorNodesDb, RocksDB minorWaysDb) throws Exception {
        var writer = Optional.ofNullable(writers.poll()).orElseThrow();
        var versions = 0L;

        var minorNodeIds = new HashSet<Long>();
        var minorMemberIds = Map.of(
                NODE, minorNodeIds,
                WAY, Sets.<Long>newHashSetWithExpectedSize(64_000));

        var changesetIds = new HashSet<Long>();
        var osh = new ArrayList<OSMRelation>();
        for (var entities : batch) {
            osh.clear();
            entities.forEach(list -> list.forEach(osm -> osh.add((OSMRelation) osm)));

            if (hasNoTags(osh) || filter(osh, keyFilter)) {
                continue;
            }

            osh.forEach(osm -> {
                osm.members().stream()
                        .filter(member -> member.type() != RELATION)
                        .forEach(member -> minorMemberIds.get(member.type()).add(member.id()));
                changesetIds.add(osm.changeset());
            });

        }

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

        for (var entities : batch) {
            osh.clear();
            entities.forEach(list -> list.forEach(osm -> osh.add((OSMRelation) osm)));

            if (hasNoTags(osh) || filter(osh, keyFilter)) {
                continue;
            }

            var time = System.nanoTime();
            var id = osh.getFirst().id();
            var contributions = new ContributionsRelation(osh, Contributions.memberOf(minorNodes, minorWays));
            var converter = new ContributionsAvroConverter(contributions, changesets::get, spatialJoiner);
            while (converter.hasNext()) {
                var contrib = converter.next();
                writer.write(contrib);
                versions++;
            }
            writer.log("%d,%d,%d".formatted(id, versions, System.nanoTime() - time));
        }

        writers.add(writer);
        return versions;
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
        var main = new Contributions2Parquet2();
        var exit = new CommandLine(main).execute(args);
        System.exit(exit);
    }

}
