package org.heigit.ohsome.replication;

import org.heigit.ohsome.changesets.ChangesetDB;
import org.heigit.ohsome.changesets.IChangesetDB;
import org.heigit.ohsome.replication.rocksdb.UpdateStoreRocksDb;
import org.heigit.ohsome.replication.state.ChangesetStateManager;
import org.heigit.ohsome.replication.state.ContributionStateManager;
import org.heigit.ohsome.replication.state.IChangesetStateManager;
import org.heigit.ohsome.replication.state.IContributionStateManager;
import org.heigit.ohsome.replication.utils.Waiter;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;


public class ReplicationManager {

    private ReplicationManager() {
        // utility class
    }

    public static int initChangesets(Path changesetsPath, String changesetDbUrl, boolean overwrite) throws IOException, SQLException {
        try (var changesetDb = new ChangesetDB(changesetDbUrl)) {
            changesetDb.createTablesIfNotExists();

            if (overwrite) {
                changesetDb.truncateChangesetTables();
            }

            var changesetManager = new ChangesetStateManager(changesetDb);
            changesetManager.initDbWithXML(changesetsPath);
            return 0;
        }
    }


    public static int update(Path directory, Path out, String replicationEndpoint, String changesetDbUrl, String replicationChangesetUrl, boolean continuous, boolean justChangesets, boolean justContributions) throws Exception {
        var lock = new ReentrantLock();
        lock.lock();
        var shutdownInitiated = new AtomicBoolean(false);
        var shutdownHook = new Thread(() -> {
            shutdownInitiated.set(true);
            lock.lock();
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        try (
                var updateStore = !justChangesets ? UpdateStoreRocksDb.open(directory, 10 << 20, true) : UpdateStore.noop();
                var changesetDb = !justContributions ? new ChangesetDB(changesetDbUrl) : IChangesetDB.noop()
        ) {
            var contributionManager = !justChangesets ? ContributionStateManager.openManager(replicationEndpoint, directory, out, updateStore, changesetDb) : IContributionStateManager.noop();
            var changesetManager = !justContributions ? new ChangesetStateManager(replicationChangesetUrl, (ChangesetDB) changesetDb) : IChangesetStateManager.noop();

            changesetManager.initializeLocalState();
            contributionManager.initializeLocalState();

            var waiter = new Waiter(
                    changesetManager.getLocalState(),
                    contributionManager.getLocalState(),
                    shutdownInitiated
            );

            do {
                var remoteChangesetState = changesetManager.fetchRemoteState();

                if (waiter.optionallyWaitAndTryAgain(remoteChangesetState)) {
                    continue;
                }

                waiter.registerLastContributionState(contributionManager);

                fetchChangesets(changesetManager);
                contributionManager.updateTowardsRemoteState();
            } while (!shutdownInitiated.get() && continuous);
        } finally {
            lock.unlock();
        }
        return 0;
    }

    private static void fetchChangesets(IChangesetStateManager changesetManager) {
        changesetManager.updateTowardsRemoteState();
        changesetManager.updateUnclosedChangesets();
    }

    public static int update(Path directory, Path out, String replicationEndpoint, boolean continuous) throws Exception {
        return update(directory, out, replicationEndpoint, null, null, continuous, false, true);
    }

    public static int update(String changesetDbUrl, String replicationChangesetsUrl, boolean continuous) throws Exception {
        return update(null, null, null, changesetDbUrl, replicationChangesetsUrl, continuous, true, false);
    }
}
