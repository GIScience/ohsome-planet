package org.heigit.ohsome.replication;

import org.heigit.ohsome.contributions.FileInfo;
import org.heigit.ohsome.osm.pbf.OSMPbf;
import org.heigit.ohsome.replication.databases.ChangesetDB;
import org.heigit.ohsome.replication.databases.KeyValueDB;
import org.heigit.ohsome.replication.state.ChangesetStateManager;
import org.heigit.ohsome.replication.state.ContributionStateManager;
import org.heigit.ohsome.replication.utils.Waiter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static org.heigit.ohsome.contributions.util.Utils.getBlobHeaders;


public class ReplicationManager {

    public static int initElements(Path pbfPath, Path out, String replicationUrl, int parallel) throws IOException {
        Files.createDirectories(out);

        var pbf = OSMPbf.open(pbfPath);
        FileInfo.printInfo(pbf);
        var blobHeaders = getBlobHeaders(pbf);
        var blobTypes = pbf.blobsByType(blobHeaders);

//        try (var ch = FileChannel.open(pbf.path(), READ)) {
//            var init = new InitElements(ch, parallel, blobTypes);
//            for (var type : OSMType.values()) {
//                var bla = new Bla(type);
//                init.init(type, bla);
//                System.out.printf("%s maxTimestamp = %s%n", type, bla.maxTimestamp());
//                System.out.printf("%s elements = %s%n", type, bla.elements());
//            }
//        }

        return 0;
    }


    private ReplicationManager() {
        // utility class
    }

    public static int init(Path changesetsPath, String changesetDbUrl) throws IOException {
        try (var changesetDb = new ChangesetDB(changesetDbUrl)) {
            var changesetManager = new ChangesetStateManager(changesetDb);
            changesetManager.initDbWithXML(changesetsPath);
            return 0;
        }
    }

    public static int update(Path directory, Path out, String changesetDbUrl) throws Exception {
        return update(directory, out, ContributionStateManager.PLANET_OSM_MINUTELY, changesetDbUrl, ChangesetStateManager.CHANGESET_ENDPOINT, true);
    }

    public static int update(Path directory, Path out, String replicationEndpoint, String changesetDbUrl, String replicationChangesetUrl, boolean continuous) throws Exception {
        var lock = new ReentrantLock();
        lock.lock();
        var shutdownInitiated = new AtomicBoolean(false);
        var shutdownHook = new Thread(() -> {
            shutdownInitiated.set(true);
            lock.lock();
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        try (var keyValueDB = new KeyValueDB(directory);
             var changesetDb = new ChangesetDB(changesetDbUrl)) {
            var changesetManager = new ChangesetStateManager(replicationChangesetUrl, changesetDb);
            var contributionManager = ContributionStateManager.openManager(replicationEndpoint, directory, out, changesetDb);

            changesetManager.initializeLocalState();
            contributionManager.initializeLocalState();

            var waiter = new Waiter(changesetManager.getLocalState(), contributionManager.getLocalState(), shutdownInitiated);

            do {
                var remoteChangesetState = changesetManager.fetchRemoteState();

                if (waiter.optionallyWaitAndTryAgain(remoteChangesetState)) {
                    continue;
                }
                waiter.registerLastContributionState(contributionManager.fetchRemoteState());

                fetchChangesets(changesetManager);
                // todo: if (justChangesets) {continue;}
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
