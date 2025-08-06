package org.heigit.ohsome.replication.state;

import org.heigit.ohsome.contributions.util.OscParser;
import org.heigit.ohsome.osm.OSMEntity;

import java.io.InputStream;
import java.time.Instant;
import java.util.Iterator;

public class ContributionStateManager extends AbstractStateManager<OSMEntity> {
    public static final String CONTRIBUTION_ENDPOINT = "https://planet.osm.org/replication/";

    public ContributionStateManager(String interval) {
        super(CONTRIBUTION_ENDPOINT + interval + "/", "state.txt", "sequenceNumber", "timestamp", ".osc.gz");
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
