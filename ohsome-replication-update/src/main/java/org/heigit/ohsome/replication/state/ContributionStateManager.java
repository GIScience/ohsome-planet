package org.heigit.ohsome.replication.state;

import com.google.common.base.Stopwatch;
import org.apache.parquet.hadoop.ParquetWriter;
import org.heigit.ohsome.changesets.IChangesetDB;
import org.heigit.ohsome.contributions.avro.Contrib;
import org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner;
import org.heigit.ohsome.contributions.util.Utils;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.output.OutputLocation;
import org.heigit.ohsome.parquet.ParquetUtil;
import org.heigit.ohsome.parquet.avro.AvroUtil;
import org.heigit.ohsome.replication.ReplicationState;
import org.heigit.ohsome.replication.Server;
import org.heigit.ohsome.replication.UpdateStore;
import org.heigit.ohsome.replication.update.ContributionUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.time.Instant.now;
import static reactor.core.publisher.Mono.fromCallable;
import static reactor.core.scheduler.Schedulers.boundedElastic;

public class ContributionStateManager implements IContributionStateManager {

    private static final Logger logger = LoggerFactory.getLogger(ContributionStateManager.class);

    public static ContributionStateManager openManager(Path directory, OutputLocation out, UpdateStore updateStore, IChangesetDB changesetDB, SpatialJoiner countryJoiner) throws IOException {
        var localStatePath = directory.resolve("state.txt");
        var localState = loadLocalState(localStatePath);
        return new ContributionStateManager(localState.getEndpoint(), directory, localState, out, updateStore, changesetDB, countryJoiner);
    }

    private final IChangesetDB changesetDB;
    private final SpatialJoiner countryJoiner;
    private final UpdateStore updateStore;

    private final Path localStatePath;
    private final OutputLocation out;
    private final Server<OSMEntity> server;
    private final String endpoint;
    private final Path tmpDir;

    private ReplicationState localState;
    private ReplicationState remoteState;

    private AtomicBoolean shutdownInitiated;
    private int maxSize;
    private int parallel = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);

    public ContributionStateManager(String endpoint, Path directory, ReplicationState localState, OutputLocation out, UpdateStore updateStore, IChangesetDB changesetDB, SpatialJoiner countryJoiner) throws IOException {
        this.server = Server.osmEntityServer(endpoint);
        this.endpoint = endpoint;
        this.out = out;
        this.updateStore = updateStore;
        this.changesetDB = changesetDB;
        this.countryJoiner = countryJoiner;

        Files.createDirectories(directory);
        this.tmpDir = directory.resolve("tmp");
        Files.createDirectories(tmpDir);

        this.localStatePath = directory.resolve("state.txt");
        this.localState = localState;
    }

    public void initializeLocalState() throws Exception {
        if (Files.exists(localStatePath)) {
            var props = new Properties();
            try (var in = Files.newInputStream(localStatePath)) {
                props.load(in);
            }
            localState = new ReplicationState(props, "sequenceNumber", "timestamp", Instant::parse);
        }
    }

    public ReplicationState getLocalState() {
        return localState;
    }

    public ReplicationState fetchRemoteState() throws IOException, InterruptedException {
        remoteState = server.getLatestRemoteState();
        return remoteState;
    }

    public static ReplicationState loadLocalState(Path localStatePath) throws IOException {
        if (Files.exists(localStatePath)) {
            return ReplicationState.read(localStatePath);
        }
        throw new IOException("Unable to load local state from " + localStatePath);
    }

    protected void updateLocalState(ReplicationState state) throws IOException {
        var props = new Properties();
        props.put("timestamp", state.getTimestamp().toString());
        props.put("sequenceNumber", Integer.toString(state.getSequenceNumber()));
        props.put("endpoint", endpoint);
        try (var out = Files.newOutputStream(localStatePath)) {
            props.store(out, null);
        }
        logger.debug("Updated local state {}", localStatePath);
        localState = state;
    }

    public void updateToRemoteState() {
        updateToRemoteState(now());
    }

    public void updateToRemoteState(Instant processUntil) {
        var local = localState.getSequenceNumber();
        var remote = remoteState.getSequenceNumber();
        var steps = remote - local;
        logger.info("updating to remote state {} from {} ({} states)", remote, local, steps);
        var statesToUpdate = Math.min(steps, maxSize);
        var timer = Stopwatch.createStarted();
        var statesUpdated = Flux.range(local + 1, steps)
                .take(maxSize)
                .flatMapSequential(next -> fromCallable(() -> server.getRemoteState(next)).subscribeOn(boundedElastic()), 8)
                .filter((state) -> state.getTimestamp().isBefore(processUntil))
                .takeUntil(state -> shutdownInitiated.get())
                .index()
                .concatMap(state -> fromCallable(() -> process(state.getT2(), state.getT1(), statesToUpdate)))
                .count()
                .blockOptional()
                .orElseThrow();
        local = getLocalState().getSequenceNumber();
        var statesToRemote = remote - local;
        logger.info("{} states updated to state {} in {}. {}", statesUpdated, local, timer,
                statesToRemote == 0 ? "we are up-to-date with remote." : "%d states left to remote.".formatted(statesToRemote));
    }

    private Consumer<AvroUtil.AvroBuilder<Contrib>> config(ReplicationState state) {
        return builder -> builder
                    .withAdditionalMetadata("replication_base_url", state.getEndpoint())
                    .withAdditionalMetadata("replication_sequence_number", Integer.toString(state.getSequenceNumber()))
                    .withAdditionalMetadata("replication_timestamp", state.getTimestamp().toString())

                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
//                .withDictionaryPageSize(4 * ParquetWriter.DEFAULT_PAGE_SIZE)

                    .withDictionaryEncoding("osm_id", false)
                    .withDictionaryEncoding("refs.list.element", false)
                    .withBloomFilterEnabled("refs.list.element", true)

                    .withDictionaryEncoding("members.list.element.id", false)
                    .withBloomFilterEnabled("members.list.element.id", true)

                    .withMinRowCountForPageSizeCheck(1)
                    .withMaxRowCountForPageSizeCheck(2);
    }

    private int process(ReplicationState state, long index, int statesToUpdate) throws Exception {
        var stateData = state.toBytes(null);
        var tmpParquetFile = tmpDir.resolve("%d.opc.parquet".formatted(state.getSequenceNumber()));

        var timer = Stopwatch.createStarted();
        var changesetIds = new HashSet<Long>();
        var osc = new ArrayList<OSMEntity>();
        server.getElements(state).forEachRemaining( osm -> {
            changesetIds.add(osm.changeset());
            osc.add(osm);
        });
        changesetIds.add(-1L);
        var changesets = Utils.fetchChangesets(changesetIds, changesetDB);
        var openChangesets = new HashSet<Long>();
        for (var cs : changesets.values()) {
            if (cs.getClosedAt() == null) {
                openChangesets.add(cs.getId());
            }
        }

        logger.info("update {} / {} ({}/{}) with  {} changes in {}({} open) changesets ...", state.getSequenceNumber(), state.getTimestamp(), index + 1, statesToUpdate, osc.size(), changesets.size(), openChangesets.size());
        var updater = new ContributionUpdater(updateStore, changesets, countryJoiner, parallel);
        var counter = 0;
        var pendingCounter = 0;
        try (var writer = ParquetUtil.openWriter(tmpParquetFile, Contrib.getClassSchema(), config(state))) {
            for (var contrib : updater.update(osc).toIterable()) {
                if (contrib.getTags().isEmpty() && contrib.getTagsBefore().isEmpty()) {
                    continue;
                }
                writer.write(contrib);
                counter++;
                var changeset = contrib.getChangeset();
                if (changeset.getClosedAt() == null && changeset.getId() > 0) {
                    pendingCounter++;
                    // store pending
                }
            }
        }
        changesetDB.pendingChangesets(openChangesets);

        var path = out.resolve(state.getSequenceNumberPath());
        var targetParquetFile = Path.of(path + ".opc.parquet");
        logger.debug("try to move file {} ({} bytes) to {}", tmpParquetFile, Files.size(tmpParquetFile), targetParquetFile);
        try {
            out.move(tmpParquetFile, targetParquetFile);
        } catch (Exception e) {
            logger.error("move file {} to {} failed", tmpParquetFile, targetParquetFile, e);
            throw e;
        }

        var targetStateFile = Path.of(path + ".state.txt");
        out.write(targetStateFile, stateData);

        out.write(out.resolve("state.txt"), stateData);
        out.write(out.resolve("state.csv"), "sequence_number,timestamp,path%n%s,%s,%s"
                .formatted(
                        state.getSequenceNumber(),
                        state.getTimestamp(),
                        out.location(targetParquetFile))
                .getBytes());

        updater.updateStore();
        updateLocalState(state);

        logger.info("update {} / {} ({}/{}) done. {} contributions, {} pending. in {}", state.getSequenceNumber(), state.getTimestamp(),
                index + 1, statesToUpdate,
                counter, pendingCounter,
                timer);
        return state.getSequenceNumber();
    }

    public void setShutdownInitiated(AtomicBoolean shutdownInitiated) {
        this.shutdownInitiated = shutdownInitiated;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize > 0 ? maxSize : Integer.MAX_VALUE;
    }

    public void setParallel(int parallel) {
        this.parallel = parallel;
    }
}
