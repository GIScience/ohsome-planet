package org.heigit.ohsome.replication;

import org.heigit.ohsome.changesets.ChangesetDB;
import org.heigit.ohsome.changesets.IChangesetDB;
import org.heigit.ohsome.replication.rocksdb.UpdateStoreRocksDb;
import org.heigit.ohsome.replication.state.ChangesetStateManager;
import org.heigit.ohsome.replication.state.ContributionStateManager;
import org.heigit.ohsome.replication.utils.Waiter;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static java.time.Instant.now;


public class ReplicationManager {

    private ReplicationManager() {
        // utility class
    }

    private static final int WAIT_TIME = 90;


    public static int update(Path directory, Path out, String changesetDbUrl, String replicationChangesetUrl, boolean continuous) throws Exception {
        var lock = new ReentrantLock();
        var shutdownInitiated = new AtomicBoolean(false);
        initializeShutdownHook(lock, shutdownInitiated);

        try (
                var updateStore = UpdateStoreRocksDb.open(directory, 10 << 20, true);
                var changesetDb = new ChangesetDB(changesetDbUrl)
        ) {
            var contributionManager = ContributionStateManager.openManager(directory, out, updateStore, changesetDb);
            var changesetManager = new ChangesetStateManager(replicationChangesetUrl, changesetDb);

            changesetManager.initializeLocalState();
            contributionManager.initializeLocalState();
            var waiter = new Waiter(shutdownInitiated);

            do {
                var remoteChangesetState = changesetManager.fetchRemoteState();
                var remoteContributionState = contributionManager.fetchRemoteState();

                if (!remoteChangesetState.equals(changesetManager.getLocalState())) {
                    changesetManager.updateToRemoteState();
                    changesetManager.updateUnclosedChangesets();
                    waiter.resetRetry();
                }

                if (!remoteChangesetState.equals(contributionManager.getLocalState())
                        && Waiter.notWaitingForChangesets(remoteContributionState, remoteChangesetState)) {
                    contributionManager.updateToRemoteState();
                    waiter.resetRetry();
                }

                waitForReplication(remoteChangesetState, remoteContributionState, waiter);
            } while (!shutdownInitiated.get() && continuous);
        } finally {
            lock.unlock();
        }
        return 0;
    }

    private static void waitForReplication(ReplicationState remoteChangesetState, ReplicationState remoteContributionState, Waiter waiter) throws InterruptedException {
        var timeSinceLastChangesetState = now().getEpochSecond() - remoteChangesetState.getTimestamp().getEpochSecond();
        var timeSinceLastContributionState = now().getEpochSecond() - remoteContributionState.getTimestamp().getEpochSecond();

        if (timeSinceLastChangesetState < WAIT_TIME) {
            waiter.sleep(WAIT_TIME - timeSinceLastChangesetState);
        } else if (timeSinceLastContributionState < WAIT_TIME) {
            waiter.sleep(WAIT_TIME - timeSinceLastContributionState);
        } else {
            waiter.waitForRetry();
        }
    }

    public static int updateContributions(Path directory, Path out, boolean continuous) throws Exception {
        var lock = new ReentrantLock();
        var shutdownInitiated = new AtomicBoolean(false);
        initializeShutdownHook(lock, shutdownInitiated);

        try (var updateStore = UpdateStoreRocksDb.open(directory, 10 << 20, true)) {
            var waiter = new Waiter(shutdownInitiated);
            var contributionManager = ContributionStateManager.openManager(directory, out, updateStore, IChangesetDB.noop());
            contributionManager.initializeLocalState();

            do {
                var remoteState = contributionManager.fetchRemoteState();
                if (!remoteState.equals(contributionManager.getLocalState())) {
                    contributionManager.updateToRemoteState();
                    waiter.resetRetry();

                    var timeSinceLastReplication = now().getEpochSecond() - remoteState.getTimestamp().getEpochSecond();
                    if (timeSinceLastReplication < WAIT_TIME) {
                        waiter.sleep(WAIT_TIME - timeSinceLastReplication);
                    }
                } else {
                    waiter.waitForRetry();
                }
            } while (!shutdownInitiated.get() && continuous);
        } finally {
            lock.unlock();
        }
        return 0;
    }

    public static int updateChangesets(String changesetDbUrl, String replicationChangesetUrl, boolean continuous) throws Exception {
        var lock = new ReentrantLock();
        var shutdownInitiated = new AtomicBoolean(false);
        initializeShutdownHook(lock, shutdownInitiated);

        try (var changesetDb = new ChangesetDB(changesetDbUrl)) {
            var waiter = new Waiter(shutdownInitiated);
            var changesetManager = new ChangesetStateManager(replicationChangesetUrl, changesetDb);
            changesetManager.initializeLocalState();

            do {
                var remoteState = changesetManager.fetchRemoteState();

                if (!remoteState.equals(changesetManager.getLocalState())) {
                    changesetManager.updateToRemoteState();
                    changesetManager.updateUnclosedChangesets();
                    waiter.resetRetry();

                    var timeSinceLastReplication = now().getEpochSecond() - remoteState.getTimestamp().getEpochSecond();
                    if (timeSinceLastReplication < WAIT_TIME) {
                        waiter.sleep(WAIT_TIME - timeSinceLastReplication);
                    }
                } else {
                    waiter.waitForRetry();
                }
            } while (!shutdownInitiated.get() && continuous);
        } finally {
            lock.unlock();
        }
        return 0;
    }

    private static void initializeShutdownHook(ReentrantLock lock, AtomicBoolean shutdownInitiated) {
        lock.lock();
        var shutdownHook = new Thread(() -> {
            shutdownInitiated.set(true);
            lock.lock();
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }
}
