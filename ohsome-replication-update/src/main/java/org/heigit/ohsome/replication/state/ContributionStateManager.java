package org.heigit.ohsome.replication.state;

import com.google.common.base.Stopwatch;
import org.heigit.ohsome.changesets.IChangesetDB;
import org.heigit.ohsome.contributions.avro.Contrib;
import org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.parquet.ParquetUtil;
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

import static java.time.Instant.now;
import static reactor.core.publisher.Mono.fromCallable;
import static reactor.core.scheduler.Schedulers.boundedElastic;

public class ContributionStateManager implements IContributionStateManager {

    private static final Logger logger = LoggerFactory.getLogger(ContributionStateManager.class);

    public static ContributionStateManager openManager(Path directory, Path out, UpdateStore updateStore, IChangesetDB changesetDB, SpatialJoiner countryJoiner) throws IOException {
        var localStatePath = directory.resolve("state.txt");
        var localState = loadLocalState(localStatePath);
        return new ContributionStateManager(localState.getEndpoint(), directory, localState, out, updateStore, changesetDB,  countryJoiner);
    }

    private final IChangesetDB changesetDB;
    private final SpatialJoiner countryJoiner;
    private final UpdateStore updateStore;

    private final Path localStatePath;
    private final Path out;
    ReplicationState localState;
    ReplicationState remoteState;
    Server<OSMEntity> server;
    String endpoint;
    Path directory;

    public ContributionStateManager(String endpoint, Path directory, ReplicationState localState, Path out, UpdateStore updateStore, IChangesetDB changesetDB, SpatialJoiner countryJoiner) throws IOException {
        server = Server.osmEntityServer(endpoint);
        this.endpoint = endpoint;
        this.directory = directory;
        this.out = out;
        this.updateStore = updateStore;
        this.changesetDB = changesetDB;
        this.countryJoiner = countryJoiner;

        Files.createDirectories(directory);
        Files.createDirectories(out);
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

    public void updateToRemoteState(){
        updateToRemoteState(now());
    }

    public void updateToRemoteState(Instant processUntil) {
        var local = localState.getSequenceNumber();
        var remote = remoteState.getSequenceNumber();
        var steps = remote - local;
        logger.info("updating to remote state {} from {} ({})", remote, local, steps);
        var timer = Stopwatch.createStarted();
        var statesUpdated = Flux.range(local + 1, steps)
                .flatMapSequential(next -> fromCallable(() -> server.getRemoteState(next)).subscribeOn(boundedElastic()), 8)
                .filter((state)-> state.getTimestamp().isBefore(processUntil))
                .concatMap(state -> fromCallable(() -> process(state)))
                .count()
                .blockOptional()
                .orElseThrow();
        logger.info("updating to remote state {} done. {} in {}", remote, statesUpdated, timer);
    }

    private int process(ReplicationState state) throws Exception {
        var stateData = state.toBytes(null);

        var tmpParquetFile = out.resolve("tmp").resolve("%d.opc.parquet".formatted(state.getSequenceNumber()));
        var tmpStateFile = out.resolve("tmp").resolve("%d.state.txt".formatted(state.getSequenceNumber()));
        Files.createDirectories(tmpParquetFile.getParent());

        Files.write(tmpStateFile, stateData);
        var timer =  Stopwatch.createStarted();
        var osc = new ArrayList<OSMEntity>();
        server.getElements(state).forEachRemaining(osc::add);
        logger.info("updating {}  with {} major changes ...", state.getSequenceNumber(), osc.size());
        var updater = new ContributionUpdater(updateStore, changesetDB, countryJoiner);
        var unclosedChangesets = new HashSet<Long>();
        var counter = 0;
        try (var writer = ParquetUtil.openWriter(tmpParquetFile, Contrib.getClassSchema(), builder -> {
            builder.withAdditionalMetadata("replication_base_url", state.getEndpoint());
            builder.withAdditionalMetadata("replication_sequence_number", Integer.toString(state.getSequenceNumber()));
            builder.withAdditionalMetadata("replication_timestamp", state.getTimestamp().toString());
        })) {
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

        var path = state.getSequenceNumberPath(out);
        Files.createDirectories(path.getParent());

        Files.move(tmpParquetFile, Path.of(path + ".opc.parquet"));
        Files.move(tmpStateFile, Path.of(path + ".state.txt"));
        Files.write(out.resolve("state.txt"), stateData);

        logger.info("update for state {} done. {} contributions, {} uncloseded. in {}", state.getSequenceNumber(), counter, unclosedChangesets.size(), timer);

        return state.getSequenceNumber();
    }
}
