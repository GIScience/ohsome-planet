package org.heigit.ohsome.replication.state;

import org.heigit.ohsome.replication.ReplicationState;

import java.io.IOException;
import java.net.URISyntaxException;

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
        public void updateToRemoteState() {
        }
    };

    static IContributionStateManager noop() {
        return NOOP;
    }

    void initializeLocalState() throws Exception;

    ReplicationState getLocalState();

    ReplicationState fetchRemoteState() throws IOException, URISyntaxException, InterruptedException;

    void updateToRemoteState();
}
