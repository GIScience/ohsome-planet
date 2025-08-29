package org.heigit.ohsome.replication.state;


import com.google.common.base.Stopwatch;
import org.heigit.ohsome.osm.changesets.OSMChangesets;
import org.heigit.ohsome.osm.changesets.PBZ2ChangesetReader;
import org.heigit.ohsome.replication.databases.ChangesetDB;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;

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
        localState = changesetDB.getLocalState();
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
        var changesets = fetchReplicationBatch(ReplicationState.sequenceNumberAsPath(nextReplication));
        changesetDB.upsertChangesets(changesets);
        System.out.println("Upserted changesets: " + changesets.size());
        updateLocalState(getRemoteReplication(nextReplication));

        return changesets.stream().filter(changeset -> !changeset.isOpen()).toList();
    }

    public List<OSMChangeset> updateUnclosedChangesets() {
        var unclosedCsIDs = changesetDB.getOpenChangesetsOlderThanTwoHours();
        var nowClosedChangesets = parse(fetchFile(
                "https://www.openstreetmap.org/api/0.6/changesets?closed=true&changesets="
                        + String.join(",", unclosedCsIDs.stream().map(Object::toString).toList())));
        changesetDB.upsertChangesets(nowClosedChangesets);
        return nowClosedChangesets;
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
