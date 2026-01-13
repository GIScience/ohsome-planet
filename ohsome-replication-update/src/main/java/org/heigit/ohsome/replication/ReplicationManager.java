package org.heigit.ohsome.replication;

import org.heigit.ohsome.changesets.ChangesetDB;
import org.heigit.ohsome.changesets.IChangesetDB;
import org.heigit.ohsome.contributions.spatialjoin.SpatialGridJoiner;
import org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner;
import org.heigit.ohsome.output.OutputLocationProvider;
import org.heigit.ohsome.replication.rocksdb.UpdateStoreRocksDb;
import org.heigit.ohsome.replication.state.ChangesetStateManager;
import org.heigit.ohsome.replication.state.ContributionStateManager;
import org.heigit.ohsome.replication.utils.Waiter;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static java.time.Instant.now;


public class ReplicationManager {

    public static final int ACCEPTABLE_DELAY = 180;

    private ReplicationManager() {
        // utility class
    }

    private static final int WAIT_TIME = 90;


    public static int update(Path countryFilePath, Path data, String out, int size, String changesetDbUrl, String replicationChangesetUrl, boolean continuous) throws Exception {
        var lock = new ReentrantLock();
        var shutdownInitiated = new AtomicBoolean(false);
        initializeShutdownHook(lock, shutdownInitiated);
        data = data.resolve("replication");

        try (var outputLocation = OutputLocationProvider.load(out)) {
            var countryJoiner = Optional.ofNullable(countryFilePath)
                    .map(SpatialGridJoiner::fromCSVGrid)
                    .orElseGet(SpatialJoiner::noop);
            try (
                    var updateStore = UpdateStoreRocksDb.open(data, 10 << 20, true);
                    var changesetDb = new ChangesetDB(changesetDbUrl)
            ) {
                var contributionManager = ContributionStateManager.openManager(data, outputLocation, updateStore, changesetDb, countryJoiner);
                var changesetManager = new ChangesetStateManager(replicationChangesetUrl, changesetDb);

                changesetManager.initializeLocalState();
                contributionManager.initializeLocalState();
                contributionManager.setMaxSize(size);

                var waiter = new Waiter(shutdownInitiated);

                do {
                    var remoteChangesetState = changesetManager.fetchRemoteState();
                    var remoteContributionState = contributionManager.fetchRemoteState();

                    if (!remoteChangesetState.equals(changesetManager.getLocalState())) {
                        changesetManager.updateToRemoteState();
                        changesetManager.updateUnclosedChangesets();
                        waiter.resetRetry();
                    }

                    if (!remoteContributionState.equals(contributionManager.getLocalState())) {
                        if (secondsBetween(remoteChangesetState, remoteContributionState) < ACCEPTABLE_DELAY) {
                            contributionManager.updateToRemoteState(remoteChangesetState.getTimestamp(), shutdownInitiated);
                        } else {
                            contributionManager.updateToRemoteState(shutdownInitiated);
                        }
                        waiter.resetRetry();
                    }

                    if (continuous) waitForReplication(remoteChangesetState, remoteContributionState, waiter);
                } while (!shutdownInitiated.get() && continuous);
            }
        } finally {
            lock.unlock();
        }
        return 0;
    }

    private static long secondsBetween(ReplicationState remoteChangesetState, ReplicationState remoteContributionState) {
        return Duration.between(remoteChangesetState.getTimestamp(), remoteContributionState.getTimestamp()).toSeconds();
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

    public static int updateContributions(Path countryFilePath, Path data, String out, int size, int parallel, boolean continuous) throws Exception {
        var lock = new ReentrantLock();
        var shutdownInitiated = new AtomicBoolean(false);
        initializeShutdownHook(lock, shutdownInitiated);
        data = data.resolve("replication");
        try (var outputLocation = OutputLocationProvider.load(out)) {
            var countryJoiner = Optional.ofNullable(countryFilePath)
                    .map(SpatialGridJoiner::fromCSVGrid)
                    .orElseGet(SpatialJoiner::noop);

            try (var updateStore = UpdateStoreRocksDb.open(data, 10 << 20, true)) {
                var waiter = new Waiter(shutdownInitiated);
                var contributionManager = ContributionStateManager.openManager(data, outputLocation, updateStore, IChangesetDB.noop(), countryJoiner);
                contributionManager.initializeLocalState();
                contributionManager.setMaxSize(size);
                contributionManager.setParallel(parallel);

                do {
                    var remoteState = contributionManager.fetchRemoteState();
                    if (!remoteState.equals(contributionManager.getLocalState())) {
                        contributionManager.updateToRemoteState(shutdownInitiated);
                        waiter.resetRetry();

                        var timeSinceLastReplication = now().getEpochSecond() - remoteState.getTimestamp().getEpochSecond();
                        if (timeSinceLastReplication < WAIT_TIME  && continuous) {
                            waiter.sleep(WAIT_TIME - timeSinceLastReplication);
                        }
                    } else {
                        waiter.waitForRetry();
                    }
                } while (!shutdownInitiated.get() && continuous);
            }
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
                    if (timeSinceLastReplication < WAIT_TIME && continuous) {
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
