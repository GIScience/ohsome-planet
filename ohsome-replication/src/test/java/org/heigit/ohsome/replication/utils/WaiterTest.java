package org.heigit.ohsome.replication.utils;

import org.heigit.ohsome.replication.state.ReplicationState;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.spy;

class WaiterTest {

    @Test
    void returnsFalseWhenChangesetReplicationIsNew() throws InterruptedException {
        var waiter = new Waiter(new ReplicationState(Instant.EPOCH, 1000), new ReplicationState(Instant.EPOCH, 1000));

        assertFalse(waiter.optionallyWaitAndTryAgain(new ReplicationState(Instant.now(), 100000)));
    }

    @Test
    void returnsTrueWhenLastChangesetReplicationFileIsRecent() throws InterruptedException {
        var changesetState = new ReplicationState(Instant.now().minusSeconds(20), 100000);

        var mockWaiter = spy(new Waiter(changesetState, new ReplicationState(Instant.EPOCH, 1000)));
        Mockito.doNothing().when(mockWaiter).waitXSeconds(anyLong());

        assertTrue(mockWaiter.optionallyWaitAndTryAgain(changesetState));
    }

    @Test
    void returnsTrueWhenLastContributionReplicationFileIsRecent() throws InterruptedException {
        var changesetState = new ReplicationState(Instant.now().minusSeconds(500), 100000);
        var contributionState = new ReplicationState(Instant.now().minusSeconds(20), 98765);

        var mockWaiter = spy(new Waiter(changesetState, contributionState));
        Mockito.doNothing().when(mockWaiter).waitXSeconds(anyLong());

        assertTrue(mockWaiter.optionallyWaitAndTryAgain(changesetState));
    }

    @Test
    void returnsFalseWhenLastContributionAndChangesetFilesAreOld() throws InterruptedException {
        var changesetState = new ReplicationState(Instant.now().minusSeconds(500), 100000);
        var contributionState = new ReplicationState(Instant.now().minusSeconds(500), 98765);

        var mockWaiter = spy(new Waiter(changesetState, contributionState));
        Mockito.doNothing().when(mockWaiter).waitXSeconds(anyLong());

        assertFalse(mockWaiter.optionallyWaitAndTryAgain(changesetState));
    }

    @Test
    void returnsAlternatingTrueFalseWhenLastContributionAndChangesetFilesAreOldAndItIsCalledRepeatedly() throws InterruptedException {
        var changesetState = new ReplicationState(Instant.now().minusSeconds(500), 100000);
        var contributionState = new ReplicationState(Instant.now().minusSeconds(500), 98765);

        var mockWaiter = spy(new Waiter(changesetState, contributionState));
        Mockito.doNothing().when(mockWaiter).waitXSeconds(anyLong());

        assertFalse(mockWaiter.optionallyWaitAndTryAgain(changesetState));
        assertTrue(mockWaiter.optionallyWaitAndTryAgain(changesetState));
        assertFalse(mockWaiter.optionallyWaitAndTryAgain(changesetState));
        assertTrue(mockWaiter.optionallyWaitAndTryAgain(changesetState));
    }

    @Test
    void waitXSecondsActuallyWaitsXSeconds() throws InterruptedException {
        var waiter = new Waiter(new ReplicationState(Instant.EPOCH, 1000), new ReplicationState(Instant.EPOCH, 1000));
        var now = Instant.now();
        waiter.waitXSeconds(1);
        var nowNow = Instant.now();
        assertTrue(now.plusSeconds(1).isBefore(nowNow));
    }
}