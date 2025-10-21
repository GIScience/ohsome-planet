package org.heigit.ohsome.replication;

import org.heigit.ohsome.contributions.FileInfo;
import org.heigit.ohsome.contributions.util.Progress;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMType;
import org.heigit.ohsome.osm.pbf.BlobHeader;
import org.heigit.ohsome.osm.pbf.OSMPbf;
import org.heigit.ohsome.replication.databases.KeyValueDB;
import org.heigit.ohsome.replication.processor.ContributionsProcessor;
import org.heigit.ohsome.replication.state.ChangesetStateManager;
import org.heigit.ohsome.replication.state.ContributionStateManager;
import org.heigit.ohsome.replication.state.ReplicationState;
import org.heigit.ohsome.replication.utils.Waiter;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.file.StandardOpenOption.READ;
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
