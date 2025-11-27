package org.heigit.ohsome.replication.state;

import org.heigit.ohsome.changesets.IChangesetDB;
import org.heigit.ohsome.contributions.avro.Contrib;
import org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.parquet.ParquetUtil;
import org.heigit.ohsome.replication.ReplicationState;
import org.heigit.ohsome.replication.Server;
import org.heigit.ohsome.replication.UpdateStore;
import org.heigit.ohsome.replication.update.ContributionUpdater;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashSet;
import java.util.Properties;

import static reactor.core.publisher.Mono.fromCallable;
import static reactor.core.scheduler.Schedulers.boundedElastic;

public class ContributionStateManager implements IContributionStateManager {

    public static ContributionStateManager openManager(String endpoint, Path directory, Path out, UpdateStore updateStore, IChangesetDB changesetDB) throws IOException {
        var localStatePath = directory.resolve("state.txt");
        var localState = loadLocalState(localStatePath);
        return new ContributionStateManager(endpoint, directory, localState, out, updateStore, changesetDB);
    }

    private final IChangesetDB changesetDB;
    private final SpatialJoiner countryJoiner = SpatialJoiner.NOOP;
    private final UpdateStore updateStore;

    private final Path localStatePath;
    private final Path out;
    ReplicationState localState;
    ReplicationState remoteState;
    Server<OSMEntity> server;
    String endpoint;
    Path directory;

    public ContributionStateManager(String endpoint, Path directory, ReplicationState localState, Path out, UpdateStore updateStore, IChangesetDB changesetDB) throws IOException {
        server = Server.osmEntityServer(endpoint);
        this.endpoint = endpoint;
        this.directory = directory;
        this.out = out;
        this.updateStore = updateStore;
        this.changesetDB = changesetDB;

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

    public ReplicationState fetchRemoteState() throws IOException, URISyntaxException, InterruptedException {
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
        System.out.println("update local state : " + state);
        localState = state;
    }

    public void updateToRemoteState() {
        var local = localState.getSequenceNumber();
        var remote = remoteState.getSequenceNumber();
        var steps = remote - local;
        var statesUpdated = Flux.range(local + 1, steps)
                .flatMapSequential(next -> fromCallable(() -> server.getRemoteState(next)).subscribeOn(boundedElastic()), 8)
                .concatMap(state -> fromCallable(() -> process(state)))
                .count()
                .blockOptional()
                .orElseThrow();
    }

    private int process(ReplicationState state) throws Exception {
        var path = state.getSequenceNumberPath(out);
        path = path.getParent().resolve(path.getFileName() + ".parquet");
        var osc = server.getElements(state);
        var updater = new ContributionUpdater(updateStore, changesetDB, countryJoiner);
        var unclosedChangesets = new HashSet<Long>();
        try (var writer = ParquetUtil.openWriter(path, Contrib.getClassSchema(), builder -> {
        })) {
            for (var contrib : updater.update(osc).toIterable()) {
                writer.write(contrib);
                var changeset = contrib.getChangeset();
                if (changeset.getClosedAt() == null &&
                        changeset.getId() > 0) {
                    // store pending
                    unclosedChangesets.add(changeset.getId());
                }
            }
        }
        changesetDB.pendingChangesets(unclosedChangesets);
        updater.updateStore();
        updateLocalState(state);
        return state.getSequenceNumber();
    }
}
