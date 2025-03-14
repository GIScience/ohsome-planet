package org.heigit.ohsome.contributions.transformer;

import com.google.common.collect.Sets;
import org.heigit.ohsome.contributions.contrib.Contribution;
import org.heigit.ohsome.contributions.contrib.Contributions;
import org.heigit.ohsome.contributions.contrib.ContributionsAvroConverter;
import org.heigit.ohsome.contributions.contrib.ContributionsRelation;
import org.heigit.ohsome.contributions.minor.MinorNodeStorage;
import org.heigit.ohsome.contributions.minor.MinorWayStorage;
import org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner;
import org.heigit.ohsome.contributions.util.Progress;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.OSMEntity.OSMRelation;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.heigit.ohsome.osm.OSMMember;
import org.heigit.ohsome.osm.OSMType;
import org.heigit.ohsome.osm.pbf.BlobHeader;
import org.heigit.ohsome.osm.pbf.BlockReader;
import org.heigit.ohsome.osm.pbf.OSMPbf;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.Iterators.peekingIterator;
import static org.heigit.ohsome.osm.OSMType.*;

public class TransformerRelations extends Transformer<OSMRelation, Long> {
    private final boolean DEBUG = false;

    private final MinorNodeStorage minorNodeStorage;
    private final MinorWayStorage minorWayStorage;
    private final SpatialJoiner countryJoiner;

    public TransformerRelations(OSMPbf pbf, Path out, int parallel, MinorNodeStorage minorNodeStorage, MinorWayStorage minorWayStorage, SpatialJoiner countryJoiner) {
        super(RELATION, pbf, out, parallel);
        this.minorNodeStorage = minorNodeStorage;
        this.minorWayStorage = minorWayStorage;
        this.countryJoiner = countryJoiner;
    }

    public static void processRelations(OSMPbf pbf, Map<OSMType, List<BlobHeader>> blobsByType, Path out, int parallel, MinorNodeStorage minorNodeStorage, MinorWayStorage minorWayStorage, SpatialJoiner countryJoiner) throws IOException {
        var transformer = new TransformerRelations(pbf, out, parallel, minorNodeStorage, minorWayStorage, countryJoiner);
        transformer.process(blobsByType);
    }

    @Override
    protected void process(Processor processor, Progress progress) throws Exception {
        try (var writer = openWriter(outputDir, osmType, builder -> builder
                .withMinRowCountForPageSizeCheck(1)
                .withMaxRowCountForPageSizeCheck(2))) {
            process(processor, progress, writer);
        }
    }

    protected void process(Processor processor, Progress progress, Parquet writer) throws Exception {
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

        var minorMemberIds = Map.of(
                NODE, new HashSet<Long>(),
                WAY, Sets.<Long>newHashSetWithExpectedSize(64_000));
        var osh = new ArrayList<OSMRelation>();
        while (offset < limit && entities.hasNext()) {
            osh.clear();
            var id = entities.peek().id();
            while (entities.hasNext() && entities.peek().id() == id) {
                osh.add((OSMRelation) entities.next());
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
                        osh.add((OSMRelation) entities.next());
                    }
                    if (entities.hasNext()) {
                        break;
                    }
                    offset++;
                    progress.step();
                }
            }

            if (!hasTags(osh)) {
                continue;
            }

            minorMemberIds.values().forEach(Set::clear);
            osh.stream()
                    .map(OSMRelation::members)
                    .<OSMMember>mapMulti(Iterable::forEach)
                    .filter(member -> member.type() != RELATION)
                    .forEach(member -> minorMemberIds.get(member.type()).add(member.id()));

            var minorWays = fetchMinorWays(minorMemberIds.get(WAY));
            var minorNodes = fetchMinorNodes(minorMemberIds.get(NODE), minorWays);

            var changesetIds = new HashSet<Long>();
            var contributionCount = 0;
            for (var contribution : (Iterable<Contribution>) () -> new ContributionsRelation(osh, Contributions.memberOf(minorNodes, minorWays))) {
                changesetIds.add(contribution.changeset());
                contributionCount++;
            }

            var changesets = fetchChangesets(changesetIds);

            var contributions = new ContributionsRelation(osh, Contributions.memberOf(minorNodes, minorWays));
            var converter = new ContributionsAvroConverter(contributions, getChangeset(changesets), countryJoiner);
            var versions = 0;
            while (converter.hasNext()) {
                var contrib = converter.next();
                writer.write(processor.id(), contrib);
                versions++;
            }
        }
    }

    private Map<Long, List<OSMWay>> fetchMinorWays(Set<Long> wayIds) {
        return minorWayStorage.getWays(wayIds);
    }

    private Map<Long, List<OSMNode>> fetchMinorNodes(Set<Long> minorNodeIds, Map<Long, List<OSMWay>> minorWays) {
        var nodes = Stream.concat(
                minorNodeIds.stream(),
                minorWays.values().stream().flatMap(Collection::stream)
                        .flatMap(osm -> osm.refs().stream())
        ).collect(Collectors.toSet());
        return minorNodeStorage.getNodes(nodes);
    }
}
