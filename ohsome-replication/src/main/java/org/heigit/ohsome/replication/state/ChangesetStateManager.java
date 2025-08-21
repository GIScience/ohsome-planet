package org.heigit.ohsome.replication.state;


import org.heigit.ohsome.replication.databases.ChangesetDB;
import org.heigit.ohsome.replication.parser.ChangesetParser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static java.net.URI.create;
import static org.heigit.ohsome.replication.parser.ChangesetParser.Changeset;

public class ChangesetStateManager extends AbstractStateManager<Changeset> {
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
    protected Iterator<Changeset> getParser(InputStream input) {
        try {
            return new ChangesetParser(
                    input
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<Changeset> updateTowardsRemoteState() {
        var nextReplication = localState.getSequenceNumber() + 1 + replicationOffset;
        var changesets = fetchReplicationBatch(ReplicationState.sequenceNumberAsPath(nextReplication));
        changesetDB.upsertChangesets(changesets);
        System.out.println("Upserted changesets: " + changesets.size());
        updateLocalState(getRemoteReplication(nextReplication));

        return changesets.stream().filter(changeset -> !changeset.open).toList();
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

    public void initDbWithXML(Path changesetsPath) {
        try (var input = new GZIPInputStream(Files.newInputStream(changesetsPath))) {
            parseAndProcessBatch(input, changesetDB::upsertChangesets, 500);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
