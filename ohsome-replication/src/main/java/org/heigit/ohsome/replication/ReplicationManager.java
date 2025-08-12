package org.heigit.ohsome.replication;

import org.heigit.ohsome.replication.state.ChangesetStateManager;
import org.heigit.ohsome.replication.state.ContributionStateManager;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;


public class ReplicationManager {

    public Integer initialize() {
        System.out.println("Not implemented");
        return 0;
    }

    public Integer updateWrapper(String interval) {
        var lock = new ReentrantLock();
        lock.lock();

        var shutdownInitiated = new AtomicBoolean(false);
        var shutdownHook = new Thread(() -> {
            shutdownInitiated.set(true);
            lock.lock();
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        try {
            var contributionManager = new ContributionStateManager(interval);
            var changesetState = new ChangesetStateManager();

            while (!shutdownInitiated.get()) {
                var localChangesetState = changesetState.getLocalState();
                var remoteChangesetState = changesetState.getRemoteState();

                if (!localChangesetState.equals(remoteChangesetState)) {
                    changesetState.updateToRemoteState();
                    var nowClosedChangesets = changesetState.updateUnclosedChangesets();
                    if (!nowClosedChangesets.isEmpty()) {
                        // todo: contributionManager.releaseUnjoinedContributions(nowClosedChangesets);
                    }
                }

                var localContributionState = contributionManager.getLocalState();
                var remoteContributionState = contributionManager.getRemoteState();


            }
        } finally {
            lock.unlock();
        }
        return 0;
    }
}
