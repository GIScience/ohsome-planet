package org.heigit.ohsome.replication.utils;

import org.heigit.ohsome.replication.state.ReplicationState;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

public class Waiter {
    ReplicationState lastChangesetState;
    ReplicationState lastContributionState;

    boolean alreadyWaited = false;
    boolean firstTimeAfterSuccess = true;

    public boolean optionallyWaitAndTryAgain(ReplicationState remoteChangesetState) {
        if (lastChangesetState == null || !lastChangesetState.equals(remoteChangesetState)) {
            lastChangesetState = remoteChangesetState;
            return false;
        }

        var now = Instant.now();
        if (remoteChangesetState.timestamp.plusSeconds(80).isAfter(now)) {
            waitForReplicationFile(now, remoteChangesetState.timestamp);
            return true;
        }

        if (lastContributionState != null && lastContributionState.timestamp.plusSeconds(80).isAfter(now)) {
            waitForReplicationFile(now, lastContributionState.timestamp);
            return true;
        }

        if (firstTimeAfterSuccess || alreadyWaited) {
            alreadyWaited = false;
            firstTimeAfterSuccess = false;
            return false;
        } else {
            waitXSeconds(60L);
            alreadyWaited = true;
            return true;
        }
    }


    void waitForReplicationFile(Instant now, Instant lastReplicationTimestamp) {
        var secondsToWait = 80 - ChronoUnit.SECONDS.between(lastReplicationTimestamp, now);
        waitXSeconds(secondsToWait);
    }

    void waitXSeconds(Long x) {
        try {
            TimeUnit.SECONDS.sleep(x);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    public void registerLastContributionState(ReplicationState remoteContributionState) {
        if (!remoteContributionState.equals(lastContributionState)) {
            lastContributionState = remoteContributionState;
            firstTimeAfterSuccess = true;
        }
    }
}
