package org.heigit.ohsome.replication.state;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

public class ChangesetStateManager extends AbstractStateManager {
    public static final String CHANGESET_ENDPOINT = "https://planet.osm.org/replication/changesets/";

    public ChangesetStateManager() {
        super(CHANGESET_ENDPOINT, "state.yaml", "sequence", "last_run");
    }

    @Override
    public Instant timestampParser(String timestamp) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS XXX");
        return OffsetDateTime.parse(timestamp, formatter).toInstant();
    }

    public ReplicationState getLocalState() {
        // todo: call to changesetMD db
        return localState;
    }

}
