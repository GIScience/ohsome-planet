package org.heigit.ohsome.replication.state;


import me.tongfei.progressbar.ProgressBarBuilder;
import org.apache.parquet.hadoop.ParquetWriter;
import org.heigit.ohsome.osm.changesets.OSMChangesets;
import org.heigit.ohsome.osm.changesets.PBZ2ChangesetReader;
import org.heigit.ohsome.parquet.ParquetUtil;
import org.heigit.ohsome.replication.databases.ChangesetDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static java.net.URI.create;
import static org.heigit.ohsome.osm.changesets.OSMChangesets.OSMChangeset;
import static org.heigit.ohsome.osm.changesets.OSMChangesets.readChangesets;
import static reactor.core.publisher.Mono.fromCallable;
import static reactor.core.publisher.Mono.fromRunnable;
import static reactor.core.scheduler.Schedulers.boundedElastic;
import static reactor.core.scheduler.Schedulers.parallel;

public class ChangesetStateManager extends AbstractStateManager<OSMChangeset> {
    private static final Logger logger = LoggerFactory.getLogger(ChangesetStateManager.class);
    public static final String CHANGESET_ENDPOINT = "https://planet.osm.org/replication/changesets/";
    private static ChangesetDB changesetDB;

    public ChangesetStateManager(ChangesetDB changesetDB) {
        this(CHANGESET_ENDPOINT, changesetDB);
    }

    public ChangesetStateManager(String endpoint, ChangesetDB changesetDB) {
        super(endpoint + "/", "state.yaml", "sequence", "last_run", ".osm.gz", 1);
        this.changesetDB = changesetDB;
    }

    @Override
    public Instant timestampParser(String timestamp) {
        return OffsetDateTime.parse(timestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS XXX")).toInstant();
    }

    @Override
    public void initializeLocalState() throws Exception {
        try {
            localState = changesetDB.getLocalState();
        } catch (NoSuchElementException e) {
            logger.info("No local state detected for changesets, trying to estimate starting replication state");
            var maxChangesetDBTimestamp = changesetDB.getMaxLocalTimestamp();
            setInitialState(
                    estimateLocalReplicationState(
                            maxChangesetDBTimestamp, fetchRemoteState()
                    )
            );
            logger.info("Estimated replication state for {}: {}", maxChangesetDBTimestamp, this.localState);
        }
    }

    protected void setInitialState(ReplicationState state) throws SQLException {
        changesetDB.setInitialState(state);
        localState = state;
    }

    @Override
    protected void updateLocalState(ReplicationState state) throws SQLException {
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

        logger.debug("Trying to update from replication state {} to {}", nextReplication, nextReplication + steps);

        Flux.range(nextReplication, steps)
                .buffer(500)
                .concatMap(batch -> fromCallable(() -> updateBatch(batch)))
                .blockLast();
    }


    private Set<Long> updateBatch(List<Integer> batch) throws IOException, SQLException {
        var closed = new HashSet<Long>();
        for (var changesets : Flux.fromIterable(batch)
                .map(ReplicationState::sequenceNumberAsPath)
                .flatMap(path -> fromCallable(() -> getFile(path)).subscribeOn(boundedElastic()), 20)
                .flatMap(file -> fromCallable(() -> parseGZIP(file)).subscribeOn(parallel()))
                .flatMap(cs -> fromCallable(() -> {
                            changesetDB.upsertChangesets(cs);
                            return cs;
                        }).subscribeOn(boundedElastic()),
                        changesetDB.getMaxConnections()
                )
                .toIterable()) {
            changesets.stream().filter(OSMChangeset::isClosed).map(OSMChangeset::id).forEach(closed::add);
        }

        var lastReplication = getRemoteReplication(batch.getLast());
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
                            return getFile(create(url).toURL());
                        }).subscribeOn(boundedElastic()),
                        20
                )
                .flatMap(file -> fromCallable(() -> parse(file)).subscribeOn(parallel()))
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


    public static void initParquetWithXML(Path changesetsPath, Path output) throws IOException {
        try (var pb = new ProgressBarBuilder()
                .setTaskName("Parsed Changesets:")
                .build();
             var writer = ParquetUtil.openWriter(output.resolve("test.parquet"), OSMChangeset.getClassSchema(), builder -> {
             })) {
            PBZ2ChangesetReader.read(changesetsPath)
                    .flatMap(bytes -> fromCallable(() -> readChangesets(bytes)))
                    .doOnNext(cs -> pb.stepBy(cs.size()))
                    .flatMap(cs -> fromRunnable(() -> writeToParquet(cs, writer)))
                    .subscribeOn(parallel())
                    .blockFirst();
        }
    }

    private static void writeToParquet(List<OSMChangeset> changesets, ParquetWriter<Object> writer) {
        for (var changeset : changesets) {
            try {
                writer.write(changeset.toGeneric());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
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
                            changesetDB.getMaxConnections()
                    )
                    .doOnComplete(pb::close)
                    .blockLast();
        }
    }
}
