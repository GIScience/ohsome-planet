package org.heigit.ohsome.replication.state;

import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.replication.databases.KeyValueDB;
import org.heigit.ohsome.replication.parser.OscParser;
import org.heigit.ohsome.replication.processor.ContributionsProcessor;

import java.io.InputStream;
import java.time.Instant;
import java.util.Iterator;

public class ContributionStateManager extends AbstractStateManager<OSMEntity> {
    public static final String CONTRIBUTION_ENDPOINT = "https://planet.osm.org/replication/";
    private final KeyValueDB keyValueDB;

    public ContributionStateManager(String interval, KeyValueDB keyValueDB) {
        super(CONTRIBUTION_ENDPOINT + interval + "/", "state.txt", "sequenceNumber", "timestamp", ".osc.gz", 0);
        this.keyValueDB = keyValueDB;
    }

    @Override
    protected Instant timestampParser(String timestamp) {
        return Instant.parse(timestamp);
    }

    @Override
    public void initializeLocalState() {
        localState = keyValueDB.getLocalState();
    }

    @Override
    protected void updateLocalState(ReplicationState state) {
        keyValueDB.updateLocalState(state);
        localState = state;
    }

    public void updateTowardsRemoteState(ContributionsProcessor processor) {
        var nextSequenceNumber = localState.getSequenceNumber() + 1;

        var entities = fetchReplicationBatch(ReplicationState.sequenceNumberAsPath(nextSequenceNumber));
        processor.update(entities, nextSequenceNumber);
        updateLocalState(getRemoteReplication(nextSequenceNumber));
    }

    @Override
    protected Iterator<OSMEntity> getParser(InputStream input) {
        try {
            return new OscParser(
                    input
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
