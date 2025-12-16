package org.heigit.ohsome.contributions.transformer;

import org.heigit.ohsome.contributions.contrib.Contribution;
import org.heigit.ohsome.contributions.contrib.ContributionsAvroConverter;
import org.heigit.ohsome.contributions.contrib.ContributionsWay;
import org.heigit.ohsome.contributions.minor.MinorNode;
import org.heigit.ohsome.contributions.minor.SstWriter;
import org.heigit.ohsome.contributions.rocksdb.RocksUtil;
import org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner;
import org.heigit.ohsome.contributions.util.Progress;
import org.heigit.ohsome.contributions.util.RocksMap;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.heigit.ohsome.osm.OSMType;
import org.heigit.ohsome.osm.changesets.Changesets;
import org.heigit.ohsome.osm.pbf.BlobHeader;
import org.heigit.ohsome.osm.pbf.BlockReader;
import org.heigit.ohsome.osm.pbf.OSMPbf;
import org.heigit.ohsome.output.OutputLocation;
import org.heigit.ohsome.replication.ReplicationEntity;
import org.heigit.ohsome.util.io.Output;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterators.peekingIterator;
import static org.heigit.ohsome.contributions.Contributions2Parquet.WRITE_PARQUET;
import static org.heigit.ohsome.contributions.util.Utils.fetchChangesets;
import static org.heigit.ohsome.contributions.util.Utils.hasNoTags;
import static org.heigit.ohsome.osm.OSMEntity.OSMNode;
import static org.heigit.ohsome.osm.OSMType.WAY;

public class TransformerWays extends Transformer {
    public static Summary processWays(OSMPbf pbf, Map<OSMType, List<BlobHeader>> blobsByType, Path temp, OutputLocation out, int parallel,
                                      RocksDB minorNodeStorage, Path rocksDbPath, LongPredicate writeMinor, SpatialJoiner countryJoiner, Changesets changesetDb, Path replicationPath, RocksDB nodeWayBackRefs) throws IOException, RocksDBException {
        Files.createDirectories(rocksDbPath);
        Files.createDirectories(replicationPath);

        var transformer = new TransformerWays(pbf, temp, out, parallel, minorNodeStorage, rocksDbPath.resolve("ingest"), writeMinor, countryJoiner, changesetDb, replicationPath.resolve("ingest"), nodeWayBackRefs);
        var summary = transformer.process(blobsByType);

        moveSstToRocksDb(rocksDbPath);
        moveSstToRocksDb(replicationPath);

        return summary;
    }


    private final RocksDB minorNodesStorage;
    private final LongPredicate writeMinor;
    private final RocksDB nodeWayBackRefs;

    public TransformerWays(OSMPbf pbf, Path temp, OutputLocation out, int parallel, RocksDB minorNodesStorage, Path sstDirectory, LongPredicate writeMinor, SpatialJoiner countryJoiner, Changesets changesetDb, Path replicationWorkDir, RocksDB nodeWayBackRefs) {
        super(WAY, pbf, temp, out, parallel, countryJoiner, changesetDb, sstDirectory, replicationWorkDir);
        this.minorNodesStorage = minorNodesStorage;
        this.writeMinor = writeMinor;
        this.nodeWayBackRefs = nodeWayBackRefs;
    }


    @Override
    protected Summary process(Processor processor, Progress progress, Parquet writer, SstWriter sstWriter, SstWriter replicationSSTWriter) throws Exception {
        var replicationLatestTimestamp = 0L;
        var replicationElementsCount = 0L;

        var ch = processor.ch();
        var blobs = processor.blobs();
        var offset = processor.offset();
        var limit = processor.limit();
        var entities = peekingIterator(BlockReader.readBlock(ch, blobs.get(offset)).entities().iterator());
        var osm = entities.peek();
        if (processor.isWithHistory() && offset > 0 && osm.version() > 1) {
            while (entities.hasNext() && entities.peek().id() == osm.id()) {
                entities.next();
                if (!entities.hasNext() && offset < limit) {
                    offset++;
                    entities = peekingIterator(BlockReader.readBlock(ch, blobs.get(offset)).entities().iterator());
                }
            }
        }
        var BATCH_SIZE = 10_000;
        var batch = new ArrayList<List<OSMWay>>(BATCH_SIZE);
        var backRefsNodeWays = new HashMap<Long, Set<Long>>();
        var outputBackRefs = new Output(4 << 10);
        while (offset < limit && entities.hasNext()) {
            batch.clear();
            backRefsNodeWays.clear();
            while (offset < limit && entities.hasNext() && batch.size() < BATCH_SIZE) {
                var osh = new ArrayList<OSMWay>();
                var id = entities.peek().id();
                while (entities.hasNext() && entities.peek().id() == id) {
                    osh.add((OSMWay) entities.next());
                    if (!entities.hasNext()) {
                        offset++;
                        progress.step();
                        if (offset < limit) {
                            entities = peekingIterator(BlockReader.readBlock(ch, blobs.get(offset)).entities().iterator());
                        }

                    }
                }
                if (!entities.hasNext()) {
                    while (offset < blobs.size()) {
                        entities = peekingIterator(BlockReader.readBlock(ch, blobs.get(offset)).entities().iterator());
                        while (entities.hasNext() && entities.peek().id() == id) {
                            osh.add((OSMWay) entities.next());
                        }
                        if (entities.hasNext()) {
                            break;
                        }
                        offset++;
                        progress.step();
                    }
                }

                if (writeMinor.test(osh.getFirst().id())) {
                    sstWriter.writeMinorWay(osh);
                }

                var last = osh.getLast();
                if (last.visible()) {
                    for (var ref : last.refs()) {
                        backRefsNodeWays.computeIfAbsent(ref, x -> new TreeSet<>()).add(last.id());
                    }
                }
                batch.add(osh);
            }

            try (var writeOpts = new WriteOptions();
                 var writeBatch = new WriteBatch()) {
                for (var entry : backRefsNodeWays.entrySet()) {
                    outputBackRefs.reset();
                    var key = RocksUtil.key(entry.getKey());
                    var set = entry.getValue();
                    ReplicationEntity.serialize(set, outputBackRefs);
                    writeBatch.merge(key, outputBackRefs.array());
                }
                nodeWayBackRefs.write(writeOpts, writeBatch);
            }


            var minorNodes = fetchMinors(batch);
            var changesetIds = new HashSet<Long>();

            for (var osh : batch) {
                var contributions = new ContributionsWay(osh, minorNodes);
                var edits = 0;
                var minorVersion = 0;
                var before = (Contribution) null;
                while (contributions.hasNext()) {
                    var contrib = contributions.next();
                    edits++;
                    changesetIds.add(contrib.changeset());
                    var entity = contrib.entity();
                    if (before == null || entity.version() != before.entity().version()) {
                        minorVersion = 0;
                    } else {
                        minorVersion++;
                    }
                    before = contrib;
                }

                if (osh.getLast().visible()) {
                    var last = osh.getLast().withMinorAndEdits(minorVersion, edits);
                    replicationLatestTimestamp = Math.max(last.timestamp().getEpochSecond(), replicationLatestTimestamp);
                    replicationElementsCount++;
                    replicationSSTWriter.write(last.id(), output -> ReplicationEntity.serialize(last, output));
                }
            }


            if (WRITE_PARQUET) {
                var changesets = fetchChangesets(changesetIds, changesetDb);
                for (var osh : batch) {
                    if (hasNoTags(osh)) {
                        continue;
                    }

                    var contributions = new ContributionsWay(osh, minorNodes);
                    var converter = new ContributionsAvroConverter(contributions, changesets::get, countryJoiner);

                    while (converter.hasNext()) {
                        var contrib = converter.next();
                        if (contrib.isPresent()) {
                            writer.write(processor.id(), contrib.get());
                        }
                    }
                }
            }
        }
        return new Summary(Instant.ofEpochSecond(replicationLatestTimestamp), replicationElementsCount);
    }

    private Map<Long, List<OSMNode>> fetchMinors(List<List<OSMWay>> batch) {
        var refs = batch.stream()
                .<OSMWay>mapMulti(Iterable::forEach)
                .<Long>mapMulti((way, down) -> way.refs().forEach(down))
                .collect(Collectors.toSet());
        return RocksMap.get(minorNodesStorage, refs, MinorNode::deserialize);
    }
}
