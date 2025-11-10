package org.heigit.ohsome.replication;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import org.heigit.ohsome.replication.databases.ChangesetDB;
import org.heigit.ohsome.replication.rocksdb.UpdateStoreRocksDb;
import org.heigit.ohsome.replication.state.ChangesetStateManager;
import org.heigit.ohsome.replication.state.ContributionStateManager;
import org.heigit.ohsome.replication.utils.Waiter;

import java.io.IOException;
import java.nio.file.Files;
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
            if (overwrite){
                changesetDb.truncateChangesetTables();
            }

            var changesetManager = new ChangesetStateManager(changesetDb);
            changesetManager.initDbWithXML(changesetsPath);
            return 0;
        }
    }

    public static int initChangesets(Path changesetsPath, Path output, boolean overwrite) throws IOException, SQLException {
        if (overwrite){
            MoreFiles.deleteRecursively(output, RecursiveDeleteOption.ALLOW_INSECURE);
            Files.createDirectories(output);
        }

        ChangesetStateManager.initParquetWithXML(changesetsPath, output);
        return 0;
    }

    public static int update(Path directory, Path out, String replicationEndpoint, String changesetDbUrl, String replicationChangesetUrl, boolean continuous, boolean justChangesets) throws Exception {
        var lock = new ReentrantLock();
        lock.lock();
        var shutdownInitiated = new AtomicBoolean(false);
        var shutdownHook = new Thread(() -> {
            shutdownInitiated.set(true);
            lock.lock();
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        try (var updateStore = UpdateStoreRocksDb.open(directory, 10 << 20, true);
             var changesetDb = new ChangesetDB(changesetDbUrl)) {
            var changesetManager = new ChangesetStateManager(replicationChangesetUrl, changesetDb);
            var contributionManager = ContributionStateManager.openManager(replicationEndpoint, directory, out, updateStore, changesetDb);

            changesetManager.initializeLocalState();
            contributionManager.initializeLocalState();

            var waiter = new Waiter(
                    changesetManager.getLocalState(),
                    contributionManager.getLocalState(),
                    shutdownInitiated,
                    justChangesets
            );

            do {
                var remoteChangesetState = changesetManager.fetchRemoteState();

                if (waiter.optionallyWaitAndTryAgain(remoteChangesetState)) {
                    continue;
                }

                waiter.registerLastContributionState(contributionManager);

                fetchChangesets(changesetManager);

                if (justChangesets) {
                    continue;
                }

                contributionManager.updateTowardsRemoteState();
            } while (!shutdownInitiated.get() && continuous);
        } finally {
            lock.unlock();
        }
        return 0;
    }

    private static void fetchChangesets(ChangesetStateManager changesetManager) {
        changesetManager.updateTowardsRemoteState();
        changesetManager.updateUnclosedChangesets();
    }

}
