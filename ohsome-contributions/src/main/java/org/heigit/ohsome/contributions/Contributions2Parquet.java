package org.heigit.ohsome.contributions;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import me.tongfei.progressbar.ProgressBarBuilder;
import org.heigit.ohsome.contributions.avro.Contrib;
import org.heigit.ohsome.contributions.contrib.Contributions;
import org.heigit.ohsome.contributions.contrib.ContributionsAvroConverter;
import org.heigit.ohsome.contributions.contrib.ContributionsRelation;
import org.heigit.ohsome.contributions.minor.MinorNode;
import org.heigit.ohsome.contributions.minor.MinorWay;
import org.heigit.ohsome.contributions.rocksdb.RocksUtil;
import org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner;
import org.heigit.ohsome.contributions.util.RocksMap;
import org.heigit.ohsome.contributions.util.Utils;
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
import org.rocksdb.RocksDBException;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static com.google.common.base.Predicates.alwaysTrue;
import static java.nio.file.StandardOpenOption.READ;
import static org.heigit.ohsome.contributions.minor.MinorNodeStorage.inRocksMap;
import static org.heigit.ohsome.contributions.transformer.TransformerNodes.processNodes;
import static org.heigit.ohsome.contributions.transformer.TransformerWays.processWays;
import static org.heigit.ohsome.contributions.util.Utils.*;
import static org.heigit.ohsome.osm.OSMType.*;
import static org.heigit.ohsome.osm.pbf.OSMPbf.blobBuffer;
import static org.heigit.ohsome.osm.pbf.OSMPbf.blockBuffer;
import static org.heigit.ohsome.osm.pbf.ProtoZero.decodeMessage;
import static reactor.core.publisher.Mono.fromCallable;
import static reactor.core.scheduler.Schedulers.parallel;

@CommandLine.Command(name = "contributions", aliases = {"contribs"},
        mixinStandardHelpOptions = true,
        version = "ohsome-planet contribution 1.0.1", //TODO version should be automatically set see picocli.CommandLine.IVersionProvider
        description = "generates parquet files")
public class Contributions2Parquet implements Callable<Integer> {

    @Option(names = {"--pbf"}, required = true)
    private Path pbfPath;

    @Option(names = {"--output"})
    private Path out = Path.of("out");

    @Option(names = {"--overwrite"})
    private boolean overwrite = false;

    @Option(names = {"--parallel"}, description = "number of threads used for processing. Dictates the number of files which will created.")
    private int parallel = Runtime.getRuntime().availableProcessors() - 1;

    @Option(names = {"--country-file"})
    private Path countryFilePath;

    @Option(names = {"--changeset-db"}, description = "full jdbc:url to changesetmd database e.g. jdbc:postgresql://HOST[:PORT]/changesets?user=USER&password=PASSWORD")
    private String changesetDbUrl = "";

    @Option(names = {"--debug"}, description = "Print debug information.")
    private boolean debug = false;

    @Option(names = {"--include-tags"}, description = "OSM keys of relations that should be built")
    private String includeTags = "";

    private SpatialJoiner countryJoiner;

    @Override
    public Integer call() throws Exception {
        var pbf = OSMPbf.open(pbfPath);
        if (debug) {
            FileInfo.printInfo(pbf);
        }

        if (Files.exists(out)) {
            if (overwrite) {
                MoreFiles.deleteRecursively(out, RecursiveDeleteOption.ALLOW_INSECURE);
            } else {
                System.out.println("Directory already exists. To overwrite use --overwrite");
                System.exit(0);
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

        if (debug) {
            printBlobInfo(blobTypes);
        }

        countryJoiner = Optional.ofNullable(countryFilePath)
                .map(SpatialJoiner::fromCSVGrid)
                .orElseGet(SpatialJoiner::noop);

        var changesetDb = Changesets.open(changesetDbUrl, parallel);

        Files.createDirectories(out);

        RocksDB.loadLibrary();
        var minorNodesPath = out.resolve("minorNodes");
        processNodes(pbf, blobTypes, out, parallel, minorNodesPath, countryJoiner, changesetDb);
        var minorWaysPath = out.resolve("minorWays");
        try (var minorNodes = inRocksMap(minorNodesPath)) {
            processWays(pbf, blobTypes, out, parallel, minorNodes, minorWaysPath, x -> true, countryJoiner, changesetDb);
        }

        processRelations(pbfPath, out, parallel, blobTypes, keyFilter, changesetDb);

        System.out.println("done in " + total);
        return 0;
    }

    private void processRelations(Path pbfPath, Path output, int numFiles, Map<OSMType, List<BlobHeader>> blobTypes, Map<String, Predicate<String>> keyFilter, Changesets changesetDb) throws IOException, InterruptedException, RocksDBException {
        try (var ch = FileChannel.open(pbfPath, READ);
             var options = RocksUtil.defaultOptions().setCreateIfMissing(true);
             var minorNodesDb = RocksDB.open(options, output.resolve("minorNodes").toString());
             var minorWaysDb = RocksDB.open(options, output.resolve("minorWays").toString());
             var progress = new ProgressBarBuilder()
                     .setTaskName("process %8s".formatted(RELATION))
                     .setInitialMax(blobTypes.get(RELATION).size())
                     .setUnit(" blk", 1)
                     .build()) {

            var readerScheduler =
                    Schedulers.newBoundedElastic(10 * Runtime.getRuntime().availableProcessors(), 10_000, "reader", 60, true);

            var writers = getWriters(output, numFiles);

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

            var canceled = new AtomicBoolean(false);
            while (entities.hasNext() && !canceled.get()) {
                var osh = getNextOSH(entities);

                if (hasNoTags(osh) || filterOut(osh, keyFilter)) {
                    continue;
                }

                var writer = writers.take();
                contribWorkers.execute(() -> {
                    try {
                        processRelation(osh, writer, countryJoiner, changesetDb, minorNodesDb, minorWaysDb, debug);
                    } catch (Exception e) {
                        canceled.set(true);
                        System.err.println(e.getMessage());
                    } finally {
                        writers.add(writer);
                    }
                });
            }

            if (canceled.get()) {
                System.err.println("cancelled");
            }
            for (var i = 0; i < numFiles; i++) {
                var writer = writers.take();
                writer.close(canceled.get());
            }
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

    private static ArrayBlockingQueue<Writer> getWriters(Path output, int numFiles) {
        var writers = new ArrayBlockingQueue<Writer>(numFiles);
        for (var i = 0; i < numFiles; i++) {
            writers.add(new Writer(i, RELATION, output, Contributions2Parquet::relationParquetConfig));
        }
        return writers;
    }

    private static void relationParquetConfig(AvroUtil.AvroBuilder<Contrib> config) {
        config.withMinRowCountForPageSizeCheck(1)
                .withMaxRowCountForPageSizeCheck(2);
    }

    private static void processRelation(List<OSMEntity> entities, Writer writer, SpatialJoiner spatialJoiner, Changesets changesetDb, RocksDB minorNodesDb, RocksDB minorWaysDb, boolean debug) throws Exception {
        var id = entities.getFirst().id();
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

        var changesets = Utils.fetchChangesets(changesetIds, changesetDb);

        var time = System.nanoTime();
        var contributions = new ContributionsRelation(osh, Contributions.memberOf(minorNodes, minorWays));
        var converter = new ContributionsAvroConverter(contributions, changesets::get, spatialJoiner);
        var versions = 0;
        while (converter.hasNext()) {
            var contrib = converter.next();
            if (contrib.isPresent()) {
                writer.write(contrib.get());
                versions++;
            }
        }

        if (debug) {
            writer.log("%s,%s,%s".formatted(id, versions, System.nanoTime() - time));
        }
    }

    private void printBlobInfo(Map<OSMType, List<BlobHeader>> blobTypes) {
        System.out.println("Blobs by type:");
        System.out.println("  Nodes: " + blobTypes.get(OSMType.NODE).size() +
                           " | Ways: " + blobTypes.get(OSMType.WAY).size() +
                           " | Relations: " + blobTypes.get(OSMType.RELATION).size()
        );
    }

    public static void main(String[] args) {
        var main = new Contributions2Parquet();
        var exit = new CommandLine(main).execute(args);
        System.exit(exit);
    }

}
