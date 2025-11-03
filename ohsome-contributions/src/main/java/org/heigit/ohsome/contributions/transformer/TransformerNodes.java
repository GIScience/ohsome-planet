package org.heigit.ohsome.contributions.transformer;

import org.heigit.ohsome.contributions.contrib.Contribution;
import org.heigit.ohsome.contributions.contrib.ContributionsAvroConverter;
import org.heigit.ohsome.contributions.contrib.ContributionsNode;
import org.heigit.ohsome.contributions.minor.SstWriter;
import org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner;
import org.heigit.ohsome.contributions.util.Progress;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.OSMType;
import org.heigit.ohsome.osm.changesets.Changesets;
import org.heigit.ohsome.osm.pbf.BlobHeader;
import org.heigit.ohsome.osm.pbf.BlockReader;
import org.heigit.ohsome.osm.pbf.OSMPbf;
import org.heigit.ohsome.replication.ReplicationEntity;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterators.peekingIterator;
import static org.heigit.ohsome.contributions.util.Utils.fetchChangesets;
import static org.heigit.ohsome.contributions.util.Utils.hasNoTags;
import static org.heigit.ohsome.osm.OSMType.NODE;

public class TransformerNodes extends Transformer {

    public TransformerNodes(OSMPbf pbf, Path temp, Path out, int parallel, Path sstDirectory, SpatialJoiner countryJoiner, Changesets changesetDb, Path sstReplicationPath) {
        super(NODE, pbf, temp, out, parallel, countryJoiner, changesetDb, sstDirectory, sstReplicationPath);
    }

    public static Summary processNodes(OSMPbf pbf, Map<OSMType, List<BlobHeader>> blobsByType, Path temp, Path out, int parallel, Path rocksDbPath, SpatialJoiner countryJoiner, Changesets changesetDb, Path replicationPath) throws IOException, RocksDBException {
        Files.createDirectories(rocksDbPath);
        Files.createDirectories(replicationPath);

        var transformer = new TransformerNodes(pbf, temp, out, parallel, rocksDbPath.resolve("ingest"), countryJoiner, changesetDb, replicationPath.resolve("ingest"));
        var summary = transformer.process(blobsByType);

        moveSstToRocksDb(rocksDbPath);
        moveSstToRocksDb(replicationPath);

        return summary;
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
                if (!entities.hasNext() && ++offset < limit) {
                    entities = peekingIterator(BlockReader.readBlock(ch, blobs.get(offset)).entities().iterator());
                }
            }
        }
        var BATCH_SIZE = 10_000;
        var batch = new ArrayList<List<OSMNode>>(BATCH_SIZE);
        while (offset < limit && entities.hasNext()) {
            batch.clear();
            while (offset < limit && entities.hasNext() && batch.size() < BATCH_SIZE) {
                var osh = new ArrayList<OSMNode>();
                var id = entities.peek().id();
                while (entities.hasNext() && entities.peek().id() == id) {
                    osh.add((OSMNode) entities.next());
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
                            osh.add((OSMNode) entities.next());
                        }
                        if (entities.hasNext()) {
                            break;
                        }
                        offset++;
                        progress.step();
                    }
                }

                sstWriter.writeMinorNode(osh);
                var last = osh.getLast();
                if (last.visible()) {
                    replicationLatestTimestamp = Math.max(last.timestamp().getEpochSecond(), replicationLatestTimestamp);
                    replicationElementsCount++;
                    replicationSSTWriter.write(last.id(), output -> ReplicationEntity.serialize(last, output));
                }

                if (hasNoTags(osh)) {
                    continue;
                }

                batch.add(osh);
            }

            var changesetIds = batch.stream()
                    .map(ContributionsNode::new)
                    .<Contribution>mapMulti(Iterator::forEachRemaining)
                    .map(Contribution::changeset)
                    .collect(Collectors.toSet());

            var changesets = fetchChangesets(changesetIds, changesetDb);

            for (var osh : batch) {
                var contributions = new ContributionsNode(osh);
                var converter = new ContributionsAvroConverter(contributions, changesets::get, countryJoiner);

                while (converter.hasNext()) {
                    var contrib = converter.next();
                    if (contrib.isPresent()) {
                        writer.write(processor.id(), contrib.get());
                    }
                }
            }
        }
        return new Summary(Instant.ofEpochSecond(replicationLatestTimestamp), replicationElementsCount);
    }
}
