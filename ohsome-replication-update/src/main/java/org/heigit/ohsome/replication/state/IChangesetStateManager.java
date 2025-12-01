package org.heigit.ohsome.replication.state;

import org.heigit.ohsome.replication.ReplicationState;

import java.io.IOException;
import java.net.URISyntaxException;

import static java.time.Instant.EPOCH;

public interface IChangesetStateManager {

    IChangesetStateManager NOOP = new IChangesetStateManager() {
        @Override
        public void updateToRemoteState() {
        }

        @Override
        public void updateUnclosedChangesets() {
        }

        @Override
        public void initializeLocalState() {
        }

        @Override
        public ReplicationState fetchRemoteState() {
            return getLocalState();
        }

        @Override
        public ReplicationState getLocalState() {
            return new ReplicationState(EPOCH, 1);
        }
    };

    static IChangesetStateManager noop() {
        return NOOP;
    }

    void updateToRemoteState();

    void updateUnclosedChangesets();

    void initializeLocalState() throws Exception;

    ReplicationState fetchRemoteState() throws IOException, URISyntaxException, InterruptedException;

    ReplicationState getLocalState();
}
