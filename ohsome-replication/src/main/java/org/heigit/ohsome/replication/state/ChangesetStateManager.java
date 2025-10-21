package org.heigit.ohsome.replication.state;


import me.tongfei.progressbar.ProgressBarBuilder;
import org.heigit.ohsome.osm.changesets.OSMChangesets;
import org.heigit.ohsome.osm.changesets.PBZ2ChangesetReader;
import org.heigit.ohsome.replication.databases.ChangesetDB;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static java.net.URI.create;
import static java.util.function.Function.identity;
import static org.heigit.ohsome.osm.changesets.OSMChangesets.OSMChangeset;
import static org.heigit.ohsome.osm.changesets.OSMChangesets.readChangesets;
import static reactor.core.publisher.Mono.fromCallable;
import static reactor.core.scheduler.Schedulers.boundedElastic;
import static reactor.core.scheduler.Schedulers.parallel;

public class ChangesetStateManager extends AbstractStateManager<OSMChangeset> {
    public static final String CHANGESET_ENDPOINT = "https://planet.osm.org/replication/changesets/";
    public ChangesetDB changesetDB;

    public ChangesetStateManager(String dbUrl) {
        this(CHANGESET_ENDPOINT, dbUrl);
    }

    public ChangesetStateManager(String endpoint, String dbUrl) {
        super(endpoint + "/", "state.yaml", "sequence", "last_run", ".osm.gz", 1);
        changesetDB = new ChangesetDB(dbUrl);
    }

    @Override
    public Instant timestampParser(String timestamp) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS XXX");
        return OffsetDateTime.parse(timestamp, formatter).toInstant();
    }

    @Override
    public void initializeLocalState() {
        try {
            localState = changesetDB.getLocalState();
        } catch (NoSuchElementException e) {
            System.out.println("No local state detected for changesets, trying to estimate starting replication state");
            var maxChangesetDBTimestamp = changesetDB.getMaxLocalTimestamp();
            updateLocalState(
                    estimateLocalReplicationState(
                            maxChangesetDBTimestamp, fetchRemoteState()
                    )
            );
            System.out.println("Estimated replication state:" + this.localState);
        }
    }

    @Override
    protected void updateLocalState(ReplicationState state) {
        changesetDB.updateState(state);
        localState = state;
    }

    @Override
    protected Iterator<OSMChangeset> getParser(InputStream input) throws IOException {
        return OSMChangesets.readChangesets(input).iterator();
    }

    public void updateTowardsRemoteState() {
        var nextReplication = localState.getSequenceNumber() + 1 + replicationOffset;
        var steps = (remoteState.getSequenceNumber() + replicationOffset + 1) - nextReplication;


        Flux.range(nextReplication, steps)
                .buffer(500)
                .concatMap(batch ->
                        Flux.fromIterable(batch)
                                .map(ReplicationState::sequenceNumberAsPath)
                                .flatMap(path ->
                                        fromCallable(() -> fetchReplicationBatch(path))
                                                .subscribeOn(boundedElastic())
                                )
                                .flatMap(cs -> fromCallable(() -> {
                                            changesetDB.upsertChangesets(cs);
                                            return cs;
                                        }).subscribeOn(boundedElastic()),
                                        5
                                )
                                .flatMapIterable(identity())
                                .filter(OSMChangeset::isClosed)
                                .collectList()
                                .doOnSuccess(ignore -> {
                                    var lastReplication = getRemoteReplication(batch.getLast());
                                    updateLocalState(lastReplication);
                                    System.out.println("Updated state up to " + lastReplication);
                                })
                ).blockLast();
    }


    public void updateUnclosedChangesets() {
        Flux.fromIterable(changesetDB.getOpenChangesetsOlderThanTwoHours())
                .buffer(100)
                .flatMap(partition -> fromCallable(() -> {
                            var url = "https://www.openstreetmap.org/api/0.6/changesets?closed=true&changesets="
                                      + partition.stream().map(String::valueOf).collect(Collectors.joining(","));
                            return fetchFile(url);
                        })
                )
                .flatMap(stream -> fromCallable(() -> parse(stream)))
                .subscribeOn(parallel())
                .flatMap(
                        cs -> fromCallable(() -> {
                            changesetDB.upsertChangesets(cs);
                            return cs;
                        }).subscribeOn(boundedElastic()),
                        5
                )
                .doOnNext(cs -> System.out.println(Instant.now() + "; Upserted previously unclosed changesets :" + cs.size()))
                .blockLast();
    }


    private InputStream fetchFile(String url) {
        try {
            return getFileStream(create(url).toURL());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
                            5 // todo: changesetDB.getMaxConnections() for some reason does not work with citus
                    )
                    .doOnComplete(pb::close)
                    .blockLast();
        }
    }
}
