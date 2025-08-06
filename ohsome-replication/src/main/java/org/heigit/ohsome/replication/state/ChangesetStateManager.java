package org.heigit.ohsome.replication.state;


import org.heigit.ohsome.osm.changesets.Changeset;
import org.heigit.ohsome.osm.changesets.ChangesetXmlReader;

import java.io.InputStream;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;

public class ChangesetStateManager extends AbstractStateManager<Changeset> {
    private static final String CHANGESET_ENDPOINT = "https://planet.osm.org/replication/changesets/";

    public ChangesetStateManager() {
        super(CHANGESET_ENDPOINT, "state.yaml", "sequence", "last_run", ".osm.gz");
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

    @Override
    protected Iterator<Changeset> getParser(InputStream input) {
        try {
            return new ChangesetXmlReader<>(
                    Changeset::new,
                    input
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
