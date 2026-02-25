package org.heigit.ohsome.contributions;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import me.tongfei.progressbar.ProgressBarBuilder;
import org.heigit.ohsome.changesets.ChangesetDB;
import org.heigit.ohsome.contributions.avro.Contrib;
import org.heigit.ohsome.contributions.contrib.Contribution;
import org.heigit.ohsome.contributions.contrib.Contributions;
import org.heigit.ohsome.contributions.contrib.ContributionsAvroConverter;
import org.heigit.ohsome.contributions.contrib.ContributionsRelation;
import org.heigit.ohsome.contributions.minor.MinorNode;
import org.heigit.ohsome.contributions.minor.MinorWay;
import org.heigit.ohsome.contributions.rocksdb.RocksUtil;
import org.heigit.ohsome.contributions.spatialjoin.SpatialGridJoiner;
import org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner;
import org.heigit.ohsome.contributions.transformer.Transformer;
import org.heigit.ohsome.contributions.util.RocksMap;
import org.heigit.ohsome.contributions.util.Utils;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMEntity.OSMRelation;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.heigit.ohsome.osm.OSMType;
import org.heigit.ohsome.osm.changesets.Changesets;
import org.heigit.ohsome.osm.pbf.Blob;
import org.heigit.ohsome.osm.pbf.BlobHeader;
import org.heigit.ohsome.osm.pbf.Block;
import org.heigit.ohsome.osm.pbf.OSMPbf;
import org.heigit.ohsome.output.OutputLocation;
import org.heigit.ohsome.parquet.AvroGeoParquetWriter.AvroGeoParquetBuilder;
import org.heigit.ohsome.replication.ReplicationEntity;
import org.heigit.ohsome.replication.ReplicationState;
import org.heigit.ohsome.replication.Server;
import org.heigit.ohsome.replication.UpdateStore;
import org.heigit.ohsome.util.io.Output;
import org.rocksdb.RocksDB;
import org.rocksdb.StringAppendOperator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Predicates.alwaysTrue;
import static java.nio.file.StandardOpenOption.READ;
import static org.heigit.ohsome.changesets.IChangesetDB.NOOP;
import static org.heigit.ohsome.contributions.rocksdb.RocksUtil.defaultOptions;
import static org.heigit.ohsome.contributions.rocksdb.RocksUtil.open;
import static org.heigit.ohsome.contributions.transformer.TransformerNodes.processNodes;
import static org.heigit.ohsome.contributions.transformer.TransformerWays.processWays;
import static org.heigit.ohsome.contributions.util.Utils.*;
import static org.heigit.ohsome.osm.OSMType.*;
import static org.heigit.ohsome.osm.pbf.OSMPbf.blobBuffer;
import static org.heigit.ohsome.osm.pbf.OSMPbf.blockBuffer;
import static org.heigit.ohsome.osm.pbf.ProtoZero.decodeMessage;
import static reactor.core.publisher.Mono.fromCallable;
import static reactor.core.scheduler.Schedulers.parallel;

public class Contributions2Parquet implements Callable<Integer> {

    private static final Logger logger = LoggerFactory.getLogger(Contributions2Parquet.class);

    public static final boolean WRITE_PARQUET = true;

    private final Path pbfPath;
    private final Path temp;
    private final OutputLocation outputLocation;
    private final int parallel;
    private final String changesetDbUrl;
    private final Path countryFilePath;
    private final Path replication;

    private final int multipolygonMembersLimit;
    private final String includeTags;


    private final URL replicationEndpoint;
    private SpatialJoiner countryJoiner;

    public Contributions2Parquet(Path pbfPath, Path data, OutputLocation outputLocation, int parallel, String changesetDbUrl, Path countryFilePath, URL replicationEndpoint, String includeTags, int multipolygonMembersLimit) throws IOException {
        this.pbfPath = pbfPath;
        this.temp = data.resolve("temp");
        this.outputLocation = outputLocation;
        this.parallel = parallel;
        this.changesetDbUrl = changesetDbUrl;
        this.countryFilePath = countryFilePath;
        this.replicationEndpoint = replicationEndpoint;
        this.includeTags = includeTags;
        this.multipolygonMembersLimit = multipolygonMembersLimit;

        Files.createDirectories(temp);
        if (replicationEndpoint != null) {
            this.replication = data.resolve("replication");
            Files.createDirectories(replication);
        } else {
            this.replication = null;
        }

    }

    private static Changesets openChangesets(String changesetDb) {
        if (changesetDb.startsWith("jdbc")) {
            return new ChangesetDB(changesetDb);
        }
        return NOOP;
    }

    @Override
    public Integer call() throws Exception {
        var pbf = OSMPbf.open(pbfPath);

        var latestState = (ReplicationState) null;
        var server = (Server<OSMEntity>) null;

        if (replicationEndpoint != null) {
            logger.debug("using replication_endpoint {}", replicationEndpoint);
            try {
                server = Server.osmEntityServer(replicationEndpoint + "/");
                latestState = server.getLatestRemoteState();
                System.out.println("latest replication state: " + latestState);
            } catch (IOException e) {
                System.err.println(e.getMessage());
                System.err.println("could not retrieve latest state for replication. " + replicationEndpoint);
                return 1;
            }
        }

        var total = Stopwatch.createStarted();

        var blobHeaders = getBlobHeaders(pbf);
        var blobTypes = pbf.blobsByType(blobHeaders);

        var keyFilter = new HashMap<String, Predicate<String>>();
        if (!includeTags.isBlank()) {
            for (var tag : includeTags.split(",")) {
                keyFilter.put(tag, alwaysTrue());
            }
        }

        countryJoiner = Optional.ofNullable(countryFilePath)
                .map(SpatialGridJoiner::fromCSVGrid)
                .orElseGet(SpatialJoiner::noop);

        try (var changesetDb = openChangesets(changesetDbUrl)) {

            Files.createDirectories(temp);

            RocksDB.loadLibrary();
            var summaryNodes = Transformer.Summary.EMPTY;
            var summaryWays = Transformer.Summary.EMPTY;


            var minorNodesPath = temp.resolve("minorNodes");

            var replicationNodesPath = UpdateStore.updatePath(replication, NODE);
            summaryNodes = processNodes(pbf, blobTypes, temp, outputLocation, parallel, minorNodesPath, countryJoiner, changesetDb, replicationNodesPath);
            var minorWaysPath = temp.resolve("minorWays");
            var replicationWaysPath = UpdateStore.updatePath(replication, WAY);
            try (var options = defaultOptions(false);
                 var minorNodes = open(options, minorNodesPath);
                 var optionsWithMerge = defaultOptions(true)
                         .setMergeOperator(new StringAppendOperator((char) 0));
                 var nodeWayBackRefs = replication != null ? open(optionsWithMerge, UpdateStore.updatePath(replication, UpdateStore.BackRefs.NODE_WAY)) : null) {
                summaryWays = processWays(pbf, blobTypes, temp, outputLocation, parallel, minorNodes, minorWaysPath, x -> true, countryJoiner, changesetDb, replicationWaysPath, nodeWayBackRefs);
            }

            var summaryRelations = processRelations(pbfPath, temp, outputLocation, replication, parallel, blobTypes, keyFilter, changesetDb);

            System.out.println("summaryNodes = " + summaryNodes);
            System.out.println("summaryWays = " + summaryWays);
            System.out.println("summaryRelations = " + summaryRelations);

            var replicationTimestamp = Stream.of(summaryNodes, summaryWays, summaryRelations)
                    .map(Transformer.Summary::replicationTimestamp)
                    .mapToLong(Instant::getEpochSecond)
                    .max().orElseThrow();
            System.out.println("replicationTimestamp = " + replicationTimestamp);

            if (replicationEndpoint != null) {
                var replicationState = server.findStartStateByTimestamp(Instant.ofEpochSecond(replicationTimestamp), latestState);
                System.out.println("replicationState = " + replicationState);
                var replicationStateData = replicationState.toBytes(server.endpoint());
                Files.write(replication.resolve("state.txt"), replicationStateData);
                outputLocation.write(outputLocation.resolve("state.txt"), replicationStateData);
            }

            System.out.println("done in " + total);
            return CommandLine.ExitCode.OK;
        }
    }

    private Transformer.Summary processRelations(Path pbfPath, Path temp, OutputLocation output, Path replication, int numFiles, Map<OSMType, List<BlobHeader>> blobTypes, Map<String, Predicate<String>> keyFilter, Changesets changesetDb) throws Exception {
        var replicationPath = (Path) null;
        if (replication != null) {
            replicationPath = UpdateStore.updatePath(replication, RELATION);
            Files.createDirectories(replicationPath);
        }

        try (var ch = FileChannel.open(pbfPath, READ);
             var options = defaultOptions(true);
             var minorNodesDb = RocksDB.open(options, temp.resolve("minorNodes").toString());
             var minorWaysDb = RocksDB.open(options, temp.resolve("minorWays").toString());

             var optionsWithMerge = defaultOptions(true)
                     .setMergeOperator(new StringAppendOperator((char) 0));
             var replicationDb = replicationPath != null ? RocksDB.open(options, replicationPath.toString()) : null;
             var nodeRelationBackRefs = replicationPath != null ? open(optionsWithMerge, UpdateStore.updatePath(replication, UpdateStore.BackRefs.NODE_RELATION)) : null;
             var wayRelationBackRefs = replicationPath != null ? open(optionsWithMerge, UpdateStore.updatePath(replication, UpdateStore.BackRefs.WAY_RELATION)) : null;

             var progress = new ProgressBarBuilder()
                     .setTaskName("process %8s".formatted(RELATION))
                     .setInitialMax(blobTypes.get(RELATION).size())
                     .setUnit(" blk", 1)
                     .build()) {

            var readerScheduler =
                    Schedulers.newBoundedElastic(10 * Runtime.getRuntime().availableProcessors(), 10_000, "reader", 60, true);

            var writers = getWriters(temp, output, numFiles);

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

            var entities = Iterators.peekingIterator(new OSMIterator(blocks, progress::stepBy));

            var replicationLatestTimestamp = 0L;
            var replicationElementsCount = 0L;
            var backRefsNodeRelation = new HashMap<Long, Set<Long>>();
            var backRefsWayRelation = new HashMap<Long, Set<Long>>();
            var outputBackRefs = new Output(4 << 10);

            var canceled = new AtomicBoolean(false);
            var batch = new ArrayList<List<OSMEntity>>(1_000);


            while (entities.hasNext() && !canceled.get()) {
                batch.clear();
                backRefsNodeRelation.clear();
                backRefsWayRelation.clear();
                while (entities.hasNext() && !canceled.get() && batch.size() < 1_000) {
                    var osh = getNextOSH(entities);
                    batch.add(osh);
                    if (replicationPath != null) {
                        var last = (OSMRelation) osh.getLast();
                        if (!last.visible()) {
                            continue;
                        }

                        for (var member : last.members()) {
                            switch (member.type()) {
                                case NODE ->
                                        backRefsNodeRelation.computeIfAbsent(member.id(), k -> new TreeSet<>()).add(last.id());
                                case WAY ->
                                        backRefsWayRelation.computeIfAbsent(member.id(), k -> new TreeSet<>()).add(last.id());
                                default -> {
                                }
                            }
                        }
                    }
                }

                if (replicationPath != null) {
                    try (var writeOpts = new WriteOptions()) {
                        try (var writeBatch = new WriteBatch()) {
                            for (var entry : backRefsNodeRelation.entrySet()) {
                                outputBackRefs.reset();
                                var key = RocksUtil.key(entry.getKey());
                                var set = entry.getValue();
                                ReplicationEntity.serialize(set, outputBackRefs);
                                writeBatch.merge(key, outputBackRefs.array());
                            }
                            nodeRelationBackRefs.write(writeOpts, writeBatch);
                        }

                        try (var writeBatch = new WriteBatch()) {
                            for (var entry : backRefsWayRelation.entrySet()) {
                                outputBackRefs.reset();
                                var key = RocksUtil.key(entry.getKey());
                                var set = entry.getValue();
                                ReplicationEntity.serialize(set, outputBackRefs);
                                writeBatch.merge(key, outputBackRefs.array());
                            }
                            wayRelationBackRefs.write(writeOpts, writeBatch);
                        }
                    }
                }

                for (var osh : batch) {

                    if (osh.getLast().visible()) {
                        replicationLatestTimestamp = Math.max(osh.getLast().timestamp().getEpochSecond(), replicationLatestTimestamp);
                        replicationElementsCount++;
                    }

                    var writer = writers.take();
                    contribWorkers.execute(() -> {
                        try {
                            processRelation(osh, keyFilter, multipolygonMembersLimit, writer, countryJoiner, changesetDb, minorNodesDb, minorWaysDb, replicationDb);
                        } catch (Exception e) {
                            canceled.set(true);
                            System.err.println(e.getMessage());
                        } finally {
                            writers.add(writer);
                        }
                    });
                }
            }

            if (canceled.get()) {
                System.err.println("cancelled");
            }
            for (var i = 0; i < numFiles; i++) {
                var writer = writers.take();
                writer.close(canceled.get());
            }

            return new Transformer.Summary(Instant.ofEpochSecond(replicationLatestTimestamp), replicationElementsCount);
        }
    }

    private List<OSMEntity> getNextOSH(PeekingIterator<OSMEntity> entities) {
        var osh = new ArrayList<OSMEntity>();
        var id = entities.peek().id();
        while (entities.hasNext() && entities.peek().id() == id) {
            osh.add(entities.next());
        }
        return osh;
    }

    private static ArrayBlockingQueue<ContribWriter> getWriters(Path temp, OutputLocation output, int numFiles) {
        var writers = new ArrayBlockingQueue<ContribWriter>(numFiles);
        for (var i = 0; i < numFiles; i++) {
            writers.add(new ContribWriter(i, RELATION, temp, output, Contributions2Parquet::relationParquetConfig));
        }
        return writers;
    }

    private static void relationParquetConfig(AvroGeoParquetBuilder<Contrib> config) {
        config.withMinRowCountForPageSizeCheck(1).withMaxRowCountForPageSizeCheck(2);
    }

    private static void processRelation(List<OSMEntity> entities, Map<String, Predicate<String>> keyFilter, int multipolygonMembersLimit, ContribWriter writer, SpatialJoiner spatialJoiner, Changesets changesetDb, RocksDB minorNodesDb, RocksDB minorWaysDb, RocksDB replicationDb) throws Exception {
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
                .<OSMWay>mapMulti(Iterable::forEach)
                .map(OSMWay::refs)
                .forEach(minorNodeIds::addAll);

        var minorNodes = RocksMap.get(minorNodesDb, minorNodeIds, MinorNode::deserialize);

        {
            var contributions = new ContributionsRelation(osh, Contributions.memberOf(minorNodes, minorWays));
            var edits = 0;
            var minorVersion = 0;
            var before = (Contribution) null;
            while (contributions.hasNext()) {
                var contrib = contributions.next();
                edits++;
                changesetIds.add(contrib.changeset());
                var entity = contrib.entity();
                if (before == null || entity.version() != before.entity().version()) {
                    minorVersion = 0;
                } else {
                    minorVersion++;
                }
                before = contrib;
            }

            if (replicationDb != null) {
                if (osh.getLast().visible()) {
                    var last = osh.getLast().withMinorAndEdits(minorVersion, edits);

                    var replicationOutput = new Output(4 << 10);
                    replicationOutput.reset();
                    ReplicationEntity.serialize(last.withMinorAndEdits(minorVersion, edits), replicationOutput);
                    var key = RocksUtil.key(last.id());
                    replicationDb.put(key, replicationOutput.array());
                }
            }

        }

        if (hasNoTags(osh) || filterOut(osh, keyFilter)) {
            return;
        }

        var changesets = Utils.fetchChangesets(changesetIds, changesetDb);
        var contributions = new ContributionsRelation(osh, Contributions.memberOf(minorNodes, minorWays));
        var converter = new ContributionsAvroConverter(contributions, changesets::get, spatialJoiner, multipolygonMembersLimit);
        while (converter.hasNext()) {
            var contrib = converter.next();
            if (contrib.isPresent()) {
                writer.write(contrib.get());
            }
        }
    }

}
