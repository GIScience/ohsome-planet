package org.heigit.ohsome.replication;

import org.heigit.ohsome.replication.databases.ChangesetDB;
import org.heigit.ohsome.replication.databases.KeyValueDB;
import org.heigit.ohsome.replication.processor.ContributionsProcessor;
import org.heigit.ohsome.replication.state.ChangesetStateManager;
import org.heigit.ohsome.replication.state.ContributionStateManager;
import org.heigit.ohsome.replication.state.ReplicationState;
import org.heigit.ohsome.replication.utils.Waiter;

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
            var waiter = new Waiter();

            changesetManager.initializeLocalState();
            contributionManager.initializeLocalState();

            while (!shutdownInitiated.get()) {
                var remoteChangesetState = changesetManager.getRemoteState();

                if (waiter.optionallyWaitAndTryAgain(remoteChangesetState)){
                    continue;
                }

                fetchChangesets(changesetManager, contribProcessor);

                var remoteContributionState = contributionManager.getRemoteState();
                waiter.registerLastContributionState(remoteContributionState);

                fetchContributions(contributionManager, remoteContributionState, contribProcessor);
            }
        } finally {
            lock.unlock();
        }
        return 0;
    }

    private static void fetchChangesets(ChangesetStateManager changesetManager, ContributionsProcessor contribProcessor) {
        while (!changesetManager.localState.equals(changesetManager.remoteState)) {
            var newClosedChangeset = changesetManager.updateTowardsRemoteState();
            contribProcessor.releaseContributions(newClosedChangeset);
        }
        var nowClosedChangesets = changesetManager.updateUnclosedChangesets();
        contribProcessor.releaseContributions(nowClosedChangesets);
    }

    private static void fetchContributions(ContributionStateManager contributionManager, ReplicationState remoteContributionState, ContributionsProcessor contribProcessor) {
        while (!contributionManager.localState.equals(remoteContributionState)) {
            contributionManager.updateTowardsRemoteState(contribProcessor);
        }
    }
}
