package org.heigit.ohsome.replication.utils;

import org.heigit.ohsome.replication.ReplicationState;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WaiterTest {

    @Test
    void waitXSecondsActuallyWaitsXSeconds() throws InterruptedException {
        var waiter = new Waiter(new AtomicBoolean(false));
        var now = Instant.now();
        waiter.sleep(1, "test");
        var nowNow = Instant.now();
        assertTrue(now.plusSeconds(1).isBefore(nowNow));
    }

    @Test
    void notWaitingForChangesetsReturnsTrueIfContributionsStateIsOlderThanChangesetState() {
        assertTrue(
                Waiter.notWaitingForChangesets(
                        new ReplicationState(Instant.parse("2025-12-01T09:54:00Z"), 1000),
                        new ReplicationState(Instant.parse("2025-12-01T09:56:00Z"), 1000)
                )
        );
    }

    @Test
    void notWaitingForChangesetsReturnsFalseIfContributionsStateUpToTwoMinutesOlderThanChangesetState() {
        assertFalse(
                Waiter.notWaitingForChangesets(
                        new ReplicationState(Instant.parse("2025-12-01T09:59:00Z"), 1000),
                        new ReplicationState(Instant.parse("2025-12-01T09:58:00Z"), 1000)
                )
        );
    }

    @Test
    void notWaitingForChangesetsReturnsTrueIfContributionsStateIsMoreThanTwoMinutesOlderThanChangesetState() {
        assertTrue(
                Waiter.notWaitingForChangesets(
                        new ReplicationState(Instant.parse("2025-12-01T10:57:00Z"), 1000),
                        new ReplicationState(Instant.parse("2025-12-01T09:59:30Z"), 1000)
                )
        );
    }
}