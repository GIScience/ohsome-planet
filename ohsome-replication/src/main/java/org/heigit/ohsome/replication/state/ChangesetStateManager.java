package org.heigit.ohsome.replication.state;


import com.google.common.base.Stopwatch;
import org.heigit.ohsome.osm.changesets.OSMChangesets;
import org.heigit.ohsome.osm.changesets.PBZ2ChangesetReader;
import org.heigit.ohsome.replication.databases.ChangesetDB;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static java.net.URI.create;
import static org.heigit.ohsome.osm.changesets.OSMChangesets.OSMChangeset;
import static org.heigit.ohsome.osm.changesets.OSMChangesets.readChangesets;

public class ChangesetStateManager extends AbstractStateManager<OSMChangeset> {
    private static final String CHANGESET_ENDPOINT = "https://planet.osm.org/replication/changesets/";
    ChangesetDB changesetDB;

    public ChangesetStateManager(String dbUrl) {
        super(CHANGESET_ENDPOINT, "state.yaml", "sequence", "last_run", ".osm.gz", 1);
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
            updateLocalState(
                    estimateLocalReplicationState(
                            changesetDB.getMaxLocalTimestamp(), fetchRemoteState()
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
    protected Iterator<OSMChangeset> getParser(InputStream input) {
        try {
            return OSMChangesets.readChangesets(
                    input
            ).iterator();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<OSMChangeset> updateTowardsRemoteState() {
        var nextReplication = localState.getSequenceNumber() + 1 + replicationOffset;
        var steps = (remoteState.getSequenceNumber() + replicationOffset + 1) - nextReplication;

        return Flux.range(nextReplication, steps)
                .buffer(500)
                .concatMap(batch ->
                        Flux.fromIterable(batch)
                                .flatMap(number ->
                                        Mono.fromCallable(() ->
                                                        Tuples.of(number, fetchReplicationBatch(ReplicationState.sequenceNumberAsPath(number)))
                                                )
                                                .subscribeOn(Schedulers.parallel())
                                                .flatMap(tuple -> Mono.fromRunnable(() ->
                                                                        changesetDB.upsertChangesets(tuple.getT2())
                                                                )
                                                                .subscribeOn(Schedulers.boundedElastic())
                                                                .thenReturn(tuple)
                                                )
                                )
                                .flatMapIterable(Tuple2::getT2)
                                .filter(OSMChangeset::isClosed)
                                .doOnComplete(() -> {
                                    var lastReplication = getRemoteReplication(batch.getLast());
                                    updateLocalState(lastReplication);
                                    System.out.println("Updated state up to " + lastReplication);
                                })
                )
                .toStream()
                .toList();
        // todo: if this fails we loose which contributions should be released - when do we catch that?
        //  we could supply the releaser to do its work after each batch before we save the state right here maybe?
    }


    public List<OSMChangeset> updateUnclosedChangesets() {
        return Flux.fromIterable(changesetDB.getOpenChangesetsOlderThanTwoHours())
                .buffer(100)
                .flatMap(partition ->
                        Mono.fromCallable(() -> {
                                    var url = "https://www.openstreetmap.org/api/0.6/changesets?closed=true&changesets="
                                            + partition.stream().map(String::valueOf).collect(Collectors.joining(","));
                                    return fetchFile(url);
                                })
                                .subscribeOn(Schedulers.parallel())
                                .map(this::parse)
                                .doOnNext(
                                        changesetDB::upsertChangesets
                                )
                                .subscribeOn(Schedulers.boundedElastic())
                                .doOnNext(cs -> System.out.println(Instant.now() +"; Upserted previously unclosed changesets :" + cs.size()))
                )
                .flatMapIterable(list -> list)
                .toStream().toList();
        // todo: same issue as above here
    }


    private InputStream fetchFile(String url) {
        try {
            return getFileStream(create(url).toURL());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void initDbWithXML(Path changesetsPath) {
        try {
            var timer = Stopwatch.createStarted();
            PBZ2ChangesetReader.read(changesetsPath)
                    .flatMap(bytes ->
                            Mono.fromCallable(() -> readChangesets(bytes))
                                    .subscribeOn(Schedulers.parallel())
                    )
                    .flatMap(changesets ->
                            changesetDB.changesets2CSV(changesets)
                                    .subscribeOn(Schedulers.parallel()))
                    .flatMap(changesets ->
                                    changesetDB.bulkInsertChangesets(changesets)
                                            .subscribeOn(Schedulers.boundedElastic()),
                            changesetDB.getMaxConnections()
                    ).then().block();
            System.out.println("Pushing changesets from Bz2 to DB took: " + timer.stop());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
