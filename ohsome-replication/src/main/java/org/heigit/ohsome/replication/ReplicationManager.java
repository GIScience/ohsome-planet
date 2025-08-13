package org.heigit.ohsome.replication;

import org.heigit.ohsome.replication.state.ChangesetStateManager;
import org.heigit.ohsome.replication.state.ContributionStateManager;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;


public class ReplicationManager {

    public int init(Path changesetsPath, String changesetDbUrl, Path pbfPath, Path directory) {
        System.setProperty("DB_URL", changesetDbUrl);
        var changesetManager = new ChangesetStateManager();
        changesetManager.initDbWithXML(changesetsPath);
        // todo: translate latest timestamp to corresponding changeset replication id?

        return 0;
    }


    public Integer update(String interval) {
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
            var changesetManager = new ChangesetStateManager();

            while (!shutdownInitiated.get()) {
                var localChangesetState = changesetManager.getLocalState();
                var remoteChangesetState = changesetManager.getRemoteState();

                if (!localChangesetState.equals(remoteChangesetState)) {
                    changesetManager.updateToRemoteState();
                    var nowClosedChangesets = changesetManager.updateUnclosedChangesets();
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
