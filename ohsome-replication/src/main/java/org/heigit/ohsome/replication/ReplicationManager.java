package org.heigit.ohsome.replication;

import org.heigit.ohsome.replication.databases.ChangesetDB;
import org.heigit.ohsome.replication.databases.KeyValueDB;
import org.heigit.ohsome.replication.processor.ContributionsProcessor;
import org.heigit.ohsome.replication.state.ChangesetStateManager;
import org.heigit.ohsome.replication.state.ContributionStateManager;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;


public class ReplicationManager {

    public int init(Path changesetsPath, String changesetDbUrl, Path pbfPath, Path directory) {
        var changesetManager = new ChangesetStateManager(changesetDbUrl);
        changesetManager.initDbWithXML(changesetsPath);
        // todo: translate latest timestamp to corresponding changeset replication id?

        return 0;
    }


    public Integer update(String interval, Path directory, String changesetDbUrl) {
        var lock = new ReentrantLock();
        lock.lock();

        var shutdownInitiated = new AtomicBoolean(false);
        var shutdownHook = new Thread(() -> {
            shutdownInitiated.set(true);
            lock.lock();
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        try {
            var contributionManager = new ContributionStateManager(interval, directory);
            var changesetManager = new ChangesetStateManager(changesetDbUrl);
            var contribProcessor = new ContributionsProcessor(new ChangesetDB(changesetDbUrl), new KeyValueDB(directory));

            while (!shutdownInitiated.get()) {
                changesetManager.initializeLocalState();
                // todo: some logic to wait for next timestamp in here?
                var remoteChangesetState = changesetManager.getRemoteState();

                if (!changesetManager.localState.equals(remoteChangesetState)) {
                    while (!changesetManager.localState.equals(changesetManager.remoteState)) {
                        var newClosedChangeset = changesetManager.updateTowardsRemoteState();
                        contribProcessor.releaseContributions(newClosedChangeset);
                    }
                    var nowClosedChangesets = changesetManager.updateUnclosedChangesets();
                    contribProcessor.releaseContributions(nowClosedChangesets);
                }
                contributionManager.initializeLocalState();
                var remoteContributionState = contributionManager.getRemoteState();
                while (!contributionManager.localState.equals(remoteContributionState)) {
                    contributionManager.updateTowardsRemoteState(contribProcessor);
                }


            }
        } finally {
            lock.unlock();
        }
        return 0;
    }

}
