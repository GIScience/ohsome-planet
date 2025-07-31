package org.heigit.ohsome.replication.state;

import java.time.Instant;

public class ContributionStateManager extends AbstractStateManager {
    public static final String CONTRIBUTION_ENDPOINT = "https://planet.osm.org/replication/";

    public ContributionStateManager(String interval) {
        super(CONTRIBUTION_ENDPOINT + interval + "/", "state.txt", "sequenceNumber", "timestamp");
    }

    @Override
    protected Instant timestampParser(String timestamp) {
        return Instant.parse(timestamp);
    }

    @Override
    protected ReplicationState getLocalState() {
        // todo: call to rockDb
        return null;
    }
}
