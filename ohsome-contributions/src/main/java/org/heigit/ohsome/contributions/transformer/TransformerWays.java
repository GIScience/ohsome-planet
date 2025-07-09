package org.heigit.ohsome.contributions.transformer;

import org.heigit.ohsome.contributions.contrib.Contribution;
import org.heigit.ohsome.contributions.contrib.ContributionsAvroConverter;
import org.heigit.ohsome.contributions.contrib.ContributionsWay;
import org.heigit.ohsome.contributions.minor.MinorNodeStorage;
import org.heigit.ohsome.contributions.minor.SstWriter;
import org.heigit.ohsome.contributions.rocksdb.RocksUtil;
import org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner;
import org.heigit.ohsome.contributions.util.Progress;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.heigit.ohsome.osm.OSMType;
import org.heigit.ohsome.osm.changesets.Changesets;
import org.heigit.ohsome.osm.pbf.BlobHeader;
import org.heigit.ohsome.osm.pbf.BlockReader;
import org.heigit.ohsome.osm.pbf.OSMPbf;
import org.rocksdb.EnvOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterators.peekingIterator;
import static org.heigit.ohsome.contributions.util.Utils.fetchChangesets;
import static org.heigit.ohsome.contributions.util.Utils.hasNoTags;
import static org.heigit.ohsome.osm.OSMEntity.OSMNode;
import static org.heigit.ohsome.osm.OSMType.WAY;

public class TransformerWays extends Transformer {
    public static void processWays(OSMPbf pbf, Map<OSMType, List<BlobHeader>> blobsByType, Path out, int parallel,
                                   MinorNodeStorage minorNodeStorage, Path rocksDbPath, LongPredicate writeMinor, SpatialJoiner countryJoiner, Changesets changesetDb) throws IOException, RocksDBException {
        Files.createDirectories(rocksDbPath);
        var transformer = new TransformerWays(pbf, out, parallel, minorNodeStorage, rocksDbPath.resolve("ingest"), writeMinor, countryJoiner, changesetDb);
        transformer.process(blobsByType);
        moveSstToRocksDb(rocksDbPath);
    }


    private final MinorNodeStorage minorNodesStorage;
    private final Path sstDirectory;
    private final LongPredicate writeMinor;

    public TransformerWays(OSMPbf pbf, Path out, int parallel, MinorNodeStorage minorNodesStorage, Path sstDirectory, LongPredicate writeMinor, SpatialJoiner countryJoiner, Changesets changesetDb) {
        super(WAY, pbf, out, parallel, countryJoiner, changesetDb);
        this.minorNodesStorage = minorNodesStorage;
        this.sstDirectory = sstDirectory;
        this.writeMinor = writeMinor;
    }

    @Override
    protected void process(Processor processor, Progress progress) throws Exception {
        try (var writer = openWriter(outputDir, osmType, builder -> {
        })) {
            process(processor, progress, writer);
        }
    }

    protected void process(Processor processor, Progress progress, Parquet writer) throws Exception {
        try (var options = RocksUtil.defaultOptions().setCreateIfMissing(true);
             var env = new EnvOptions();
             var sstWriter = new SstWriter(
                     sstDirectory.resolve("nodes-%03d.sst".formatted(processor.id())),
                     new SstFileWriter(env, options))) {
            process(processor, progress, writer, sstWriter);
        }
    }

    private void process(Processor processor, Progress progress, Parquet writer, SstWriter sstWriter) throws Exception {
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
        while (offset < limit && entities.hasNext()) {
            batch.clear();
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
                if (hasNoTags(osh)) {
                    continue;
                }
                batch.add(osh);
            }

            var minorNodes = fetchMinors(batch);
            var changesetIds = batch.stream()
                    .map(osh -> new ContributionsWay(osh, minorNodes))
                    .<Contribution>mapMulti(Iterator::forEachRemaining)
                    .map(Contribution::changeset)
                    .collect(Collectors.toSet());

            var changesets = fetchChangesets(changesetIds, changesetDb);

            for (var osh : batch) {
                var contributions = new ContributionsWay(osh, minorNodes);
                var converter = new ContributionsAvroConverter(contributions, changesets::get, countryJoiner);

                while (converter.hasNext()) {
                    var contrib = converter.next();
                    writer.write(processor.id(), contrib);
                }
            }
        }
    }

    private Map<Long, List<OSMNode>> fetchMinors(List<List<OSMWay>> batch) {
        var refs = batch.stream()
                .<OSMWay>mapMulti(Iterable::forEach)
                .<Long>mapMulti((way, down) -> way.refs().forEach(down))
                .collect(Collectors.toSet());
        return minorNodesStorage.getNodes(refs);
    }

}
