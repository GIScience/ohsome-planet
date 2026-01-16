package org.heigit.ohsome.replication.update;

import org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner;
import org.heigit.ohsome.contributions.util.Utils;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.changesets.Changesets;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ContributionUpdaterTest {

    private static final long timebase = Instant.parse("2025-10-01T00:00:00.00Z").getEpochSecond();


    @Test
    void update() throws Exception {
        var store = new UpdateStoreMap();
        var changesetsIds = Set.of(1L, 2L, 3L);
        var changesets = Utils.fetchChangesets(changesetsIds, Changesets.NOOP);
        var updater = new ContributionUpdater(store, changesets, SpatialJoiner.NOOP, Math.max(1, Runtime.getRuntime().availableProcessors() - 1));


        System.out.println("--");
        updater.update(List.of(
                node(1, 1, 1, 1),
                node(2, 1, 1, 1),
                way(23, 1, 1, 1, List.of(1L, 2L))))
                .collectList().blockOptional().orElseThrow().forEach(System.out::println);
        updater.updateStore();


        System.out.println("--");
        updater.update(List.of(
                node(1, 2, 2, 1)
        )).collectList().blockOptional().orElseThrow().forEach(System.out::println);
        updater.updateStore();


        System.out.println("--");
        updater.update(List.of(
                node(1, 3, 3, 1)
        )).collectList().blockOptional().orElseThrow().forEach(System.out::println);
        updater.updateStore();

        System.out.println("--");
        updater.update(List.of(
                node(1, 4, 4, 2),
                way(23, 2, 4, 2, List.of(1L, 2L))
        )).collectList().blockOptional().orElseThrow().forEach(System.out::println);
        updater.updateStore();

    }


    public static OSMEntity node(long id, int version, long time, long cs) {
        return new OSMEntity.OSMNode(id, version, Instant.ofEpochSecond(timebase + 60 * time), cs, 1, "", true, Map.of(), id, version);
    }

    public static OSMEntity way(long id, int version, long time, long cs, List<Long> refs) {
        return new OSMEntity.OSMWay(id, version, Instant.ofEpochSecond(timebase + 60 * time), cs, 1, "", true, Map.of(), refs);
    }

}