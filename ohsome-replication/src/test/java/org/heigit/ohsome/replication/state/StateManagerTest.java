package org.heigit.ohsome.replication.state;


import org.junit.jupiter.api.Test;


public class StateManagerTest {
    @Test
    public void testStateManagerGetRemoteReplicationState() {
        var changesetStateManager = new ChangesetStateManager();
        var contributionStateManager = new ContributionStateManager("minute");

        var changesetState = changesetStateManager.getRemoteState();
        System.out.println("changesetState = " + changesetState);
        var contributionState = contributionStateManager.getRemoteState();
        System.out.println("contributionState = " + contributionState);
    }

    @Test
    public void testStateManagerGetRemoteReplicationData() {
        var changesetStateManager = new ChangesetStateManager();
        var contributionStateManager = new ContributionStateManager("minute");

        var changesetReplicationFile = changesetStateManager.getReplicationFile("000/021/212");
        System.out.println("changesetReplicationFile = " + changesetReplicationFile);
        var contributionReplicationFile = contributionStateManager.getReplicationFile("000/021/212");
        System.out.println("contributionReplicationFile = " + contributionReplicationFile);
    }


    @Test
    public void testFetchReplicationBatch() {
        var changesetStateManager = new ChangesetStateManager();
        var batch = changesetStateManager.fetchReplicationBatch("000/021/212");
        for (var batchElement : batch) {
            System.out.println(batchElement);
        }

        var contributionStateManager = new ContributionStateManager("minute");
        var contributionBatch = contributionStateManager.fetchReplicationBatch("000/021/212");
        for (var batchElement : contributionBatch) {
            System.out.println(batchElement);
        }
    }
}