package org.heigit.ohsome.replication.state;

import java.io.IOException;

import static java.time.Instant.EPOCH;

public interface IContributionStateManager {
    IContributionStateManager NOOP = new IContributionStateManager() {
        @Override
        public void initializeLocalState() throws Exception {
        }

        @Override
        public ReplicationState getLocalState() {
            return new ReplicationState(EPOCH, 1);
        }

        @Override
        public ReplicationState fetchRemoteState() {
            return getLocalState();
        }

        @Override
        public void updateTowardsRemoteState() {
        }
    };

    static IContributionStateManager noop() {
        return NOOP;
    }

    void initializeLocalState() throws Exception;

    ReplicationState getLocalState();

    ReplicationState fetchRemoteState() throws IOException;

    void updateTowardsRemoteState();
}
