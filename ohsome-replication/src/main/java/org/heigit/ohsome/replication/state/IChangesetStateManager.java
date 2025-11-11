package org.heigit.ohsome.replication.state;

import java.io.IOException;

import static java.time.Instant.EPOCH;

public interface IChangesetStateManager {

    IChangesetStateManager NOOP = new IChangesetStateManager() {
        @Override
        public void updateTowardsRemoteState() {
        }

        @Override
        public void updateUnclosedChangesets() {
        }

        @Override
        public void initializeLocalState() throws Exception {
        }

        @Override
        public ReplicationState fetchRemoteState() throws IOException {
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


    void updateTowardsRemoteState();

    void updateUnclosedChangesets();

    void initializeLocalState() throws Exception;

    ReplicationState fetchRemoteState() throws IOException;

    ReplicationState getLocalState();
}
