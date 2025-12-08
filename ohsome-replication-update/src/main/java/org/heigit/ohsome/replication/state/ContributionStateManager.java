package org.heigit.ohsome.replication.state;

import com.google.common.base.Stopwatch;
import org.apache.parquet.hadoop.ParquetWriter;
import org.heigit.ohsome.changesets.IChangesetDB;
import org.heigit.ohsome.contributions.avro.Contrib;
import org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.output.OutputLocation;
import org.heigit.ohsome.parquet.ParquetUtil;
import org.heigit.ohsome.parquet.avro.AvroUtil;
import org.heigit.ohsome.replication.ReplicationState;
import org.heigit.ohsome.replication.Server;
import org.heigit.ohsome.replication.UpdateStore;
import org.heigit.ohsome.replication.update.ContributionUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.time.Instant.now;
import static reactor.core.publisher.Mono.fromCallable;
import static reactor.core.scheduler.Schedulers.boundedElastic;

public class ContributionStateManager implements IContributionStateManager {

    private static final Logger logger = LoggerFactory.getLogger(ContributionStateManager.class);

    public static ContributionStateManager openManager(Path directory, OutputLocation out, UpdateStore updateStore, IChangesetDB changesetDB, SpatialJoiner countryJoiner) throws IOException {
        var localStatePath = directory.resolve("state.txt");
        var localState = loadLocalState(localStatePath);
        return new ContributionStateManager(localState.getEndpoint(), directory, localState, out, updateStore, changesetDB, countryJoiner);
    }

    private final IChangesetDB changesetDB;
    private final SpatialJoiner countryJoiner;
    private final UpdateStore updateStore;

    private final Path localStatePath;
    private final OutputLocation out;
    ReplicationState localState;
    ReplicationState remoteState;
    Server<OSMEntity> server;
    String endpoint;
    Path directory;
    private AtomicBoolean shutdownInitiated;
    private int maxSize;
    private int parallel = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);

    public ContributionStateManager(String endpoint, Path directory, ReplicationState localState, OutputLocation out, UpdateStore updateStore, IChangesetDB changesetDB, SpatialJoiner countryJoiner) throws IOException {
        server = Server.osmEntityServer(endpoint);
        this.endpoint = endpoint;
        this.directory = directory;
        this.out = out;
        this.updateStore = updateStore;
        this.changesetDB = changesetDB;
        this.countryJoiner = countryJoiner;

        Files.createDirectories(directory);
        this.localStatePath = directory.resolve("state.txt");
        this.localState = localState;
    }

    public void initializeLocalState() throws Exception {
        if (Files.exists(localStatePath)) {
            var props = new Properties();
            try (var in = Files.newInputStream(localStatePath)) {
                props.load(in);
            }
            localState = new ReplicationState(props, "sequenceNumber", "timestamp", Instant::parse);
        }
    }

    public ReplicationState getLocalState() {
        return localState;
    }

    public ReplicationState fetchRemoteState() throws IOException, InterruptedException {
        remoteState = server.getLatestRemoteState();
        return remoteState;
    }

    public static ReplicationState loadLocalState(Path localStatePath) throws IOException {
        if (Files.exists(localStatePath)) {
            return ReplicationState.read(localStatePath);
        }
        return null;
    }

    protected void updateLocalState(ReplicationState state) throws IOException {
        var props = new Properties();
        props.put("timestamp", state.getTimestamp().toString());
        props.put("sequenceNumber", Integer.toString(state.getSequenceNumber()));
        props.put("endpoint", endpoint);
        try (var out = Files.newOutputStream(localStatePath)) {
            props.store(out, null);
        }
        logger.debug("Updated local state {}", localStatePath);
        localState = state;
    }

    public void updateToRemoteState() {
        updateToRemoteState(now());
    }

    public void updateToRemoteState(Instant processUntil) {
        var local = localState.getSequenceNumber();
        var remote = remoteState.getSequenceNumber();
        var steps = remote - local;
        logger.info("updating to remote state {} from {} ({})", remote, local, steps);
        var timer = Stopwatch.createStarted();
        var statesUpdated = Flux.range(local + 1, steps)
                .take(maxSize)
                .flatMapSequential(next -> fromCallable(() -> server.getRemoteState(next)).subscribeOn(boundedElastic()), 8)
                .filter((state) -> state.getTimestamp().isBefore(processUntil))
                .takeUntil(state -> shutdownInitiated.get())
                .concatMap(state -> fromCallable(() -> process(state)))
                .count()
                .blockOptional()
                .orElseThrow();
        logger.info("updating to remote state {} done. {} in {}", remote, statesUpdated, timer);
    }

    private Consumer<AvroUtil.AvroBuilder<Contrib>> config(ReplicationState state) {
        return builder -> {
            builder
                    .withAdditionalMetadata("replication_base_url", state.getEndpoint())
                    .withAdditionalMetadata("replication_sequence_number", Integer.toString(state.getSequenceNumber()))
                    .withAdditionalMetadata("replication_timestamp", state.getTimestamp().toString())

                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
//                .withDictionaryPageSize(4 * ParquetWriter.DEFAULT_PAGE_SIZE)

                    .withDictionaryEncoding("osm_id", false)
                    .withDictionaryEncoding("refs.list.element", false)
                    .withBloomFilterEnabled("refs.list.element", true)

                    .withBloomFilterEnabled("user.id", true)

                    .withBloomFilterEnabled("changeset.id", true)

                    .withDictionaryEncoding("members.list.element.id", false)
                    .withBloomFilterEnabled("members.list.element.id", true)

                    .withMinRowCountForPageSizeCheck(1)
                    .withMaxRowCountForPageSizeCheck(2);
        };
    }

    private int process(ReplicationState state) throws Exception {
        var stateData = state.toBytes(null);

        var tmpParquetFile = directory.resolve("tmp").resolve("%d.opc.parquet".formatted(state.getSequenceNumber()));
        var tmpStateFile = directory.resolve("tmp").resolve("%d.state.txt".formatted(state.getSequenceNumber()));
        Files.createDirectories(tmpParquetFile.getParent());

        Files.write(tmpStateFile, stateData);
        var timer = Stopwatch.createStarted();
        var osc = new ArrayList<OSMEntity>();
        server.getElements(state).forEachRemaining(osc::add);
        logger.info("updating {}  with {} major changes ...", state.getSequenceNumber(), osc.size());
        var updater = new ContributionUpdater(updateStore, changesetDB, countryJoiner, parallel);
        var unclosedChangesets = new HashSet<Long>();
        var counter = 0;
        try (var writer = ParquetUtil.openWriter(tmpParquetFile, Contrib.getClassSchema(), config(state))) {
            for (var contrib : updater.update(osc).toIterable()) {
                writer.write(contrib);
                counter++;
                var changeset = contrib.getChangeset();
                if (changeset.getClosedAt() == null && changeset.getId() > 0) {
                    // store pending
                    unclosedChangesets.add(changeset.getId());
                }
            }
        }
        changesetDB.pendingChangesets(unclosedChangesets);
        updater.updateStore();
        updateLocalState(state);

        var path = out.resolve(state.getSequenceNumberPath());
        out.move(tmpParquetFile, Path.of(path + ".opc.parquet"));
        out.move(tmpStateFile, Path.of(path + ".state.txt"));
        Files.write(directory.resolve("state.txt"), stateData);
        out.move(directory.resolve("state.txt"), Path.of(path + "state.txt"));

        logger.info("update for state {} done. {} contributions, {} uncloseded. in {}", state.getSequenceNumber(), counter, unclosedChangesets.size(), timer);

        return state.getSequenceNumber();
    }

    public void setShutdownInitiated(AtomicBoolean shutdownInitiated) {
        this.shutdownInitiated = shutdownInitiated;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize > 0 ? maxSize : Integer.MAX_VALUE;
    }

    public void setParallel(int parallel) {
        this.parallel = parallel;
    }
}
