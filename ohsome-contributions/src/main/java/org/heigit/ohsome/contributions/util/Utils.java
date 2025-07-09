package org.heigit.ohsome.contributions.util;

import com.google.common.collect.Iterables;
import me.tongfei.progressbar.ProgressBarBuilder;
import org.heigit.ohsome.contributions.avro.ContribChangeset;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.changesets.Changesets;
import org.heigit.ohsome.osm.pbf.BlobHeader;
import org.heigit.ohsome.osm.pbf.OSMPbf;

import java.time.Instant;
import java.util.*;
import java.util.function.Predicate;

import static com.google.common.base.Predicates.alwaysFalse;

public class Utils {

    private Utils() {}

    public static <T extends OSMEntity> boolean hasNoTags(List<T> osh) {
        return Iterables.all(osh, osm -> osm.tags().isEmpty());
    }

    public static <T extends OSMEntity> boolean filterOut(List<T> osh, Map<String, Predicate<String>> keyFilter) {
        if (keyFilter.isEmpty()) return false;
        return osh.stream()
                .map(OSMEntity::tags)
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .noneMatch(tag -> keyFilter.getOrDefault(tag.getKey(), alwaysFalse()).test(tag.getValue()));
    }

    public static List<BlobHeader> getBlobHeaders(OSMPbf pbf) {
        var blobHeaders = new ArrayList<BlobHeader>();
        try (var progress = new ProgressBarBuilder()
                .setTaskName("read blocks")
                .setInitialMax(pbf.size())
                .setUnit(" MiB", 1L << 20)
                .build()) {
            pbf.blobs().forEach(blobHeader -> {
                progress.stepTo(blobHeader.offset() + blobHeader.dataSize());
                blobHeaders.add(blobHeader);
            });
            progress.setExtraMessage(blobHeaders.size() + " blocks");
        }
        return blobHeaders;
    }

    public static Map<Long, ContribChangeset> fetchChangesets(Set<Long> ids, Changesets changesetDb) throws Exception {
        var changesetBuilder = ContribChangeset.newBuilder();
        var changesets = changesetDb.changesets(ids, (id, created, closed, tags, hashtags, editor, numChanges) ->
                changesetBuilder
                        .setId(id)
                        .setCreatedAt(created)
                        .setClosedAt(closed)
                        .setTags(Map.copyOf(tags))
                        .setHashtags(List.copyOf(hashtags))
                        .setEditor(editor)
                        .setNumChanges(numChanges)
                        .build());
        changesetBuilder
                .setCreatedAt(Instant.ofEpochSecond(0))
                .clearClosedAt().clearTags().clearHashtags().clearEditor().clearNumChanges();
        ids.forEach(id -> changesets.computeIfAbsent(id, x -> changesetBuilder.setId(id).build()));
        return changesets;
    }
}
