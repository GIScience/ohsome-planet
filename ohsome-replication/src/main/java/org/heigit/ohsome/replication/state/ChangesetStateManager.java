package org.heigit.ohsome.replication.state;


import org.heigit.ohsome.replication.databases.ChangesetDB;
import org.heigit.ohsome.replication.parser.ChangesetParser;

import java.io.InputStream;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;

import static java.net.URI.create;
import static java.time.Instant.EPOCH;
import static org.heigit.ohsome.replication.parser.ChangesetParser.Changeset;

public class ChangesetStateManager extends AbstractStateManager<Changeset> {
    private static final String CHANGESET_ENDPOINT = "https://planet.osm.org/replication/changesets/";
    ChangesetDB changesetDB = new ChangesetDB(System.getProperty("DB_URL"));

    public ChangesetStateManager() {
        super(CHANGESET_ENDPOINT, "state.yaml", "sequence", "last_run", ".osm.gz");
    }

    @Override
    public Instant timestampParser(String timestamp) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS XXX");
        return OffsetDateTime.parse(timestamp, formatter).toInstant();
    }

    @Override
    public ReplicationState getLocalState() {
        localState = changesetDB.getLocalState();
        return localState;
    }

    @Override
    protected void updateLocalState(ReplicationState state) {
        changesetDB.updateState(state);
        localState = state;
    }

    @Override
    protected Iterator<Changeset> getParser(InputStream input) {
        try {
            return new ChangesetParser(
                    input
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void updateToRemoteState() {
        for (int i = 0; i < remoteState.sequenceNumber - localState.sequenceNumber; i++) {
            var changesets = fetchReplicationBatch(ReplicationState.sequenceNumberAsPath(localState.sequenceNumber + 1 + i));
            changesetDB.upsertChangesets(changesets);
            changesetDB.updateState(new ReplicationState(EPOCH, localState.sequenceNumber + i));
            System.out.println("Upserted changesets: " + changesets.size());
        }
        changesetDB.updateState(remoteState);
    }

    public List<Changeset> updateUnclosedChangesets() {
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
}
