package org.heigit.ohsome.replication.utils;

import org.heigit.ohsome.replication.state.IContributionStateManager;
import org.heigit.ohsome.replication.ReplicationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Waiter {
    private static final Logger logger = LoggerFactory.getLogger(Waiter.class);
    private final AtomicBoolean shutdownInitiated;
    private static final int WAIT_TIME = 70;

    private ReplicationState lastChangesetState;
    private ReplicationState lastContributionState;

    private boolean alreadyWaited = false;
    private boolean firstTimeAfterSuccess = true;

    public Waiter(ReplicationState localChangesetState, ReplicationState localContributionState, AtomicBoolean shutdownInitiated) {
        lastChangesetState = localChangesetState;
        lastContributionState = localContributionState;
        this.shutdownInitiated = shutdownInitiated;
    }

    public boolean optionallyWaitAndTryAgain(ReplicationState remoteChangesetState) throws InterruptedException {
        if (!lastChangesetState.equals(remoteChangesetState)) {
            logger.info("--Waiter: New remote changeset state detected!");
            lastChangesetState = remoteChangesetState;
            reset();
            return false;
        }

        var now = Instant.now();
        if (remoteChangesetState.getTimestamp().plusSeconds(WAIT_TIME).isAfter(now)) {
            logger.info("--Waiter: Waiting for new remote changeset state!");
            waitForReplicationFile(now, remoteChangesetState.getTimestamp());
            return true;
        }

        if (lastContributionState.getTimestamp().plusSeconds(WAIT_TIME).isAfter(now)) {
            logger.info("--Waiter: Waiting for new remote contribution state!");
            waitForReplicationFile(now, lastContributionState.getTimestamp());
            return true;
        }

        if (firstTimeAfterSuccess || alreadyWaited) {
            logger.info("--Waiter: Trying to get new state after {}", firstTimeAfterSuccess ? "processing." : "already having waited.");
            alreadyWaited = false;
            firstTimeAfterSuccess = false;
            return false;
        }
        logger.info("--Waiter: Waiting {} seconds until trying again.", WAIT_TIME);
        waitXSeconds(WAIT_TIME);
        alreadyWaited = true;
        return true;
    }

    private void reset() {
        alreadyWaited = false;
        firstTimeAfterSuccess = true;
    }


    private void waitForReplicationFile(Instant now, Instant lastReplicationTimestamp) throws InterruptedException {
        var secondsToWait = WAIT_TIME - ChronoUnit.SECONDS.between(lastReplicationTimestamp, now);
        logger.debug("--Waiter: Waiting {} seconds until trying again.", secondsToWait);
        waitXSeconds(secondsToWait);
    }

    protected void waitXSeconds(long x) throws InterruptedException {
        for (var i = 0; i < x; i++) {
            TimeUnit.SECONDS.sleep(1);
            if (shutdownInitiated.get()) {
                logger.warn("--Waiter:  got interrupted");
                throw new InterruptedException("Interrupted during waiting. Gracefully shutting down.");
            }
        }
    }

    public void registerLastContributionState(IContributionStateManager contributionStateManager) throws IOException {
        var remoteContributionState = contributionStateManager.fetchRemoteState();
        if (!remoteContributionState.equals(lastContributionState)) {
            lastContributionState = remoteContributionState;
            reset();
        }
    }
}
