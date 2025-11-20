package org.heigit.ohsome.replication.state;


import me.tongfei.progressbar.ProgressBarBuilder;
import org.heigit.ohsome.changesets.ChangesetDB;
import org.heigit.ohsome.osm.changesets.PBZ2ChangesetReader;
import org.heigit.ohsome.replication.ReplicationState;
import org.heigit.ohsome.replication.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

import static java.net.URI.create;
import static org.heigit.ohsome.osm.changesets.OSMChangesets.OSMChangeset;
import static org.heigit.ohsome.osm.changesets.OSMChangesets.readChangesets;
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
    public ReplicationState fetchRemoteState() throws IOException {
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

    public void updateTowardsRemoteState() {
        var nextReplication = localState.getSequenceNumber() + 1 + server.getReplicationOffset();
        var steps = remoteState.getSequenceNumber() - localState.getSequenceNumber();

        logger.debug("Trying to update from replication state {} to {}", nextReplication, nextReplication + steps);

        Flux.range(nextReplication, steps)
                .buffer(500)
                .concatMap(batch -> fromCallable(() -> updateBatch(batch)))
                .blockLast();
    }


    private Set<Long> updateBatch(List<Integer> batch) throws IOException, SQLException {
        var closed = new HashSet<Long>();
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
        logger.info("Updated state up to {}", lastReplication);
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


    public void initDbWithXML(Path changesetsPath) throws IOException {
        try (var pb = new ProgressBarBuilder()
                .setTaskName("Parsed Changesets:")
                .build()) {
            PBZ2ChangesetReader.read(changesetsPath)
                    .flatMap(bytes -> fromCallable(() -> readChangesets(bytes)).subscribeOn(parallel()))
                    .doOnNext(cs -> pb.stepBy(cs.size()))
                    .flatMap(cs -> fromCallable(() -> changesetDB.changesets2CSV(cs)).subscribeOn(parallel()))
                    .flatMap(
                            changesets -> fromCallable(() -> {
                                changesetDB.bulkInsertChangesets(changesets);
                                return changesets;
                            }).subscribeOn(boundedElastic()),
                            changesetDB.getMaxConnections()
                    )
                    .doOnComplete(pb::close)
                    .blockLast();
        }
    }
}
