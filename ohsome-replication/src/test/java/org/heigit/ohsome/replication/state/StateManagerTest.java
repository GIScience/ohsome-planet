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
}