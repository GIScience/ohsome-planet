package org.heigit.ohsome.replication;

import org.heigit.ohsome.replication.databases.ChangesetDB;
import org.heigit.ohsome.replication.databases.KeyValueDB;
import org.heigit.ohsome.replication.processor.ContributionsProcessor;
import org.heigit.ohsome.replication.state.ChangesetStateManager;
import org.heigit.ohsome.replication.state.ContributionStateManager;
import org.heigit.ohsome.replication.state.ReplicationState;
import org.heigit.ohsome.replication.utils.Waiter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;


public class ReplicationManager {

    private ReplicationManager() {
        // utility class
    }

    public static int init(Path changesetsPath, String changesetDbUrl) throws IOException {
        var changesetManager = new ChangesetStateManager(changesetDbUrl);
        changesetManager.initDbWithXML(changesetsPath);

        return 0;
    }

    public static int update(Path directory, String changesetDbUrl) throws Exception {
        return update(directory, changesetDbUrl, ChangesetStateManager.CHANGESET_ENDPOINT);
    }

    public static int update(Path directory, String changesetDbUrl, String replicationChangesetUrl) throws Exception {
        var lock = new ReentrantLock();
        lock.lock();

        var shutdownInitiated = new AtomicBoolean(false);
        var shutdownHook = new Thread(() -> {
            shutdownInitiated.set(true);
            lock.lock();
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        try (var keyValueDB = new KeyValueDB(directory)) {
            var changesetManager = new ChangesetStateManager(replicationChangesetUrl, changesetDbUrl);

            var contribProcessor = new ContributionsProcessor();
            var contributionManager = ContributionStateManager.openManager(directory);

            changesetManager.initializeLocalState();
            contributionManager.initializeLocalState();

            var waiter = new Waiter(changesetManager.getLocalState(), contributionManager.getLocalState());

            while (!shutdownInitiated.get()) {
                var remoteChangesetState = changesetManager.fetchRemoteState();

                if (waiter.optionallyWaitAndTryAgain(remoteChangesetState)) {
                    continue;
                }
                var remoteContributionState = contributionManager.fetchRemoteState();
                waiter.registerLastContributionState(remoteContributionState);

                fetchChangesets(changesetManager);
                // todo: if (justChangesets) {continue;}
                fetchContributions(contributionManager, remoteContributionState, contribProcessor);
            }
        } finally {
            lock.unlock();
        }
        return 0;
    }

    private static void fetchChangesets(ChangesetStateManager changesetManager) {
        changesetManager.updateTowardsRemoteState();
        changesetManager.updateUnclosedChangesets();
    }

    private static void fetchContributions(ContributionStateManager contributionManager, ReplicationState remoteContributionState, ContributionsProcessor contribProcessor) throws Exception {
        while (!contributionManager.getLocalState().equals(remoteContributionState)) {
            contributionManager.updateTowardsRemoteState(contribProcessor);
        }
    }
}
