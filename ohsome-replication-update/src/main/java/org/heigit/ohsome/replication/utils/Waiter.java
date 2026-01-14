package org.heigit.ohsome.replication.utils;

import org.heigit.ohsome.replication.ReplicationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Waiter {
    private static final Logger logger = LoggerFactory.getLogger(Waiter.class);
    private final AtomicBoolean shutdownInitiated;


    private static final int BASE_RETRY = 5;
    private int retrySeconds = BASE_RETRY;

    public Waiter(AtomicBoolean shutdownInitiated) {
        this.shutdownInitiated = shutdownInitiated;
    }

    public void sleep(long secondsToWait, String reason) throws InterruptedException {
        logger.info("Waiting {} seconds for {} until trying again.", secondsToWait, reason);
        for (var i = 0; i < secondsToWait; i++) {
            TimeUnit.SECONDS.sleep(1);
            if (shutdownInitiated.get()) {
                logger.warn("Waiter:  got interrupted");
                throw new InterruptedException("Interrupted during waiting. Gracefully shutting down.");
            }
        }
    }

    public static boolean notWaitingForChangesets(ReplicationState lastContributionState, ReplicationState lastChangesetState) {
        return lastContributionState.getTimestamp().isBefore(lastChangesetState.getTimestamp())
                || Duration.between(lastChangesetState.getTimestamp(), lastContributionState.getTimestamp()).toSeconds() > 120;
    }

    public void waitForRetry() throws InterruptedException {
        sleep(retrySeconds, "any state to change");
        retrySeconds = Math.min(60, retrySeconds * 2);
    }

    public void resetRetry() {
        retrySeconds = BASE_RETRY;
    }
}
