package org.heigit.ohsome.replication.state;


import com.google.common.base.Stopwatch;
import org.heigit.ohsome.changesets.ChangesetDB;
import org.heigit.ohsome.replication.ReplicationState;
import org.heigit.ohsome.replication.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

import static java.net.URI.create;
import static org.heigit.ohsome.osm.changesets.OSMChangesets.OSMChangeset;
import static reactor.core.publisher.Mono.fromCallable;
import static reactor.core.scheduler.Schedulers.boundedElastic;
import static reactor.core.scheduler.Schedulers.parallel;

public class ChangesetStateManager implements IChangesetStateManager {
    private static final Logger logger = LoggerFactory.getLogger(ChangesetStateManager.class);
    private static ChangesetDB changesetDB;
    private static Server<OSMChangeset> server;
    private ReplicationState localState;
    private ReplicationState remoteState;

    public ChangesetStateManager(ChangesetDB changesetDB) {
        this.changesetDB = changesetDB;
        server = Server.OSM_CHANGESET_SERVER;
    }

    public ChangesetStateManager(String endpoint, ChangesetDB changesetDB) {
        this.changesetDB = changesetDB;
        server = Server.osmChangesetServer(endpoint);
    }

    public void initializeLocalState() throws Exception {
        try {
            localState = changesetDB.getLocalState();
        } catch (NoSuchElementException e) {
            logger.info("No local state detected for changesets, trying to estimate starting replication state");
            var maxChangesetDBTimestamp = changesetDB.getMaxLocalTimestamp();
            setInitialState(
                    server.findStartStateByTimestamp(
                            maxChangesetDBTimestamp, fetchRemoteState()
                    )
            );
            logger.info("Estimated replication state for {}: {}", maxChangesetDBTimestamp, this.localState);
        }
    }

    @Override
    public ReplicationState fetchRemoteState() throws IOException, InterruptedException {
        remoteState = server.getLatestRemoteState();
        return remoteState;
    }

    @Override
    public ReplicationState getLocalState() {
        return localState;
    }

    protected void setInitialState(ReplicationState state) throws SQLException {
        changesetDB.setInitialState(state);
        localState = state;
    }

    protected void updateLocalState(ReplicationState state) throws SQLException {
        changesetDB.updateState(state);
        localState = state;
    }

    public void updateToRemoteState() {
        var nextReplication = localState.getSequenceNumber() + 1 + server.getReplicationOffset();
        var steps = remoteState.getSequenceNumber() - localState.getSequenceNumber();

        if (steps == 0) {
            return;
        }

        logger.debug("Trying to update from replication state {} to {}", localState.getSequenceNumber(), localState.getSequenceNumber() + steps);

        Flux.range(nextReplication, steps)
                .buffer(500)
                .concatMap(batch -> fromCallable(() -> updateBatch(batch)))
                .blockLast();
    }


    private Set<Long> updateBatch(List<Integer> batch) throws IOException, SQLException, InterruptedException {
        var closed = new HashSet<Long>();
        var stopwatch = Stopwatch.createStarted();
        for (var changesets : Flux.fromIterable(batch)
                .flatMap(path -> fromCallable(() -> server.getReplicationFile(path)).subscribeOn(boundedElastic()), 20)
                .flatMap(file -> fromCallable(() -> server.parseToList(file)).subscribeOn(parallel()))
                .flatMap(cs -> fromCallable(() -> {
                            changesetDB.upsertChangesets(cs);
                            return cs;
                        }).subscribeOn(boundedElastic()),
                        changesetDB.getMaxConnections()
                )
                .toIterable()) {
            changesets.stream().filter(OSMChangeset::isClosed).map(OSMChangeset::id).forEach(closed::add);
        }

        var lastReplication = server.getRemoteState(batch.getLast());
        updateLocalState(lastReplication);
        logger.info("Updated state to {} from {} -> {} states in {}", lastReplication.getSequenceNumber() - batch.size(), lastReplication.getSequenceNumber(), batch.size(), stopwatch);
        return closed;
    }


    public void updateUnclosedChangesets() {
        Flux.fromIterable(changesetDB.getOpenChangesetsOlderThanTwoHours())
                .buffer(100)
                .flatMap(partition -> fromCallable(() -> {
                            var url = "https://www.openstreetmap.org/api/0.6/changesets?closed=true&changesets="
                                    + partition.stream().map(String::valueOf).collect(Collectors.joining(",", "", ""));
                            return Server.getFile(create(url).toURL());
                        }).subscribeOn(boundedElastic()),
                        20
                )
                .flatMap(file -> fromCallable(() -> server.parseToList(file)).subscribeOn(parallel()))
                .flatMap(
                        cs -> fromCallable(() -> {
                            changesetDB.upsertChangesets(cs);
                            return cs;
                        }).subscribeOn(boundedElastic()),
                        changesetDB.getMaxConnections()

                )
                .doOnNext(cs -> logger.info("Upserted {} previously unclosed changesets", cs.size()))
                .blockLast();
    }
}
