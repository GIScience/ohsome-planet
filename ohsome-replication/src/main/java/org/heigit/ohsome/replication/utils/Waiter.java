package org.heigit.ohsome.replication.utils;

import org.heigit.ohsome.replication.state.ReplicationState;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

public class Waiter {
    private ReplicationState lastChangesetState;
    private ReplicationState lastContributionState;

    private boolean alreadyWaited = false;
    private boolean firstTimeAfterSuccess = true;

    public Waiter(ReplicationState localChangesetState, ReplicationState localContributionState) {
        lastChangesetState = localChangesetState;
        lastContributionState = localContributionState;
    }

    public boolean optionallyWaitAndTryAgain(ReplicationState remoteChangesetState) {
        if (!lastChangesetState.equals(remoteChangesetState)) {
            lastChangesetState = remoteChangesetState;
            reset();
            return false;
        }

        var now = Instant.now();
        if (remoteChangesetState.timestamp.plusSeconds(80).isAfter(now)) {
            waitForReplicationFile(now, remoteChangesetState.timestamp);
            return true;
        }

        if (lastContributionState.timestamp.plusSeconds(80).isAfter(now)) {
            waitForReplicationFile(now, lastContributionState.timestamp);
            return true;
        }

        if (firstTimeAfterSuccess || alreadyWaited) {
            alreadyWaited = false;
            firstTimeAfterSuccess = false;
            return false;
        }

        waitXSeconds(60);
        alreadyWaited = true;
        return true;
    }

    private void reset(){
        alreadyWaited = false;
        firstTimeAfterSuccess = true;
    }


    private void waitForReplicationFile(Instant now, Instant lastReplicationTimestamp) {
        var secondsToWait = 80 - ChronoUnit.SECONDS.between(lastReplicationTimestamp, now);
        waitXSeconds(secondsToWait);
    }

    protected void waitXSeconds(long x) {
        try {
            TimeUnit.SECONDS.sleep(x);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void registerLastContributionState(ReplicationState remoteContributionState) {
        if (!remoteContributionState.equals(lastContributionState)) {
            lastContributionState = remoteContributionState;
            reset();
        }
    }
}
