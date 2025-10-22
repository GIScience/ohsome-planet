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

    public boolean optionallyWaitAndTryAgain(ReplicationState remoteChangesetState) throws InterruptedException {
        if (!lastChangesetState.equals(remoteChangesetState)) {
            System.out.println("--Waiter: New remote changeset state detected!");
            lastChangesetState = remoteChangesetState;
            reset();
            return false;
        }

        var now = Instant.now();
        if (remoteChangesetState.getTimestamp().plusSeconds(80).isAfter(now)) {
            System.out.println("--Waiter: Waiting for new remote changeset state!");
            waitForReplicationFile(now, remoteChangesetState.getTimestamp());
            return true;
        }

        if (lastContributionState.getTimestamp().plusSeconds(80).isAfter(now)) {
            System.out.println("--Waiter: Waiting for new remote contribution state!");
            waitForReplicationFile(now, lastContributionState.getTimestamp());
            return true;
        }

        if (firstTimeAfterSuccess || alreadyWaited) {
            System.out.println("--Waiter: Trying to get new state after " + (firstTimeAfterSuccess ? "processing." : "already having waited."));
            alreadyWaited = false;
            firstTimeAfterSuccess = false;
            return false;
        }
        System.out.println("--Waiter: Waiting 60 seconds until trying again.");
        waitXSeconds(60);
        alreadyWaited = true;
        return true;
    }

    private void reset() {
        alreadyWaited = false;
        firstTimeAfterSuccess = true;
    }


    private void waitForReplicationFile(Instant now, Instant lastReplicationTimestamp) throws InterruptedException {
        var secondsToWait = 80 - ChronoUnit.SECONDS.between(lastReplicationTimestamp, now);
        waitXSeconds(secondsToWait);
    }

    protected void waitXSeconds(long x) throws InterruptedException {
        try {
            TimeUnit.SECONDS.sleep(x);
        } catch (InterruptedException ignored) {
            System.out.println("--Waiter:  got interrupted");
            throw new InterruptedException("Interupted in during waiting");
        }
    }

    public void registerLastContributionState(ReplicationState remoteContributionState) {
        if (!remoteContributionState.equals(lastContributionState)) {
            lastContributionState = remoteContributionState;
            reset();
        }
    }
}
