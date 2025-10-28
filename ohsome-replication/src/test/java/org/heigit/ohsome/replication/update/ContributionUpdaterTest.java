package org.heigit.ohsome.replication.update;

import org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner;
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
    void update() {
        var store = new UpdateStore();
        var updater = new ContributionUpdater(store, Changesets.NOOP, SpatialJoiner.NOOP);
        var diffs = updater.update(List.of(
                node(1, 1, 1, 1),
                node(2, 1, 1, 1),
                way(23, 1, 1, 1, List.of(1L, 2L)))).collectList().block();
        updater.updateStore();

        diffs.forEach(System.out::println);

        var n1WayBackRefs = store.getNodeWayBackRefs(Set.of(1L));
        System.out.println("n1WayBackRefs = " + n1WayBackRefs);


        diffs = updater.update(List.of(
                node(1, 2, 2, 2)
        )).collectList().block();

        diffs.forEach(System.out::println);


    }


    public static OSMEntity node(long id, int version, long time, long cs) {
        return new OSMEntity.OSMNode(id, version, Instant.ofEpochSecond(timebase + 60 * time), cs, 1, "", true, Map.of(), id, version);
    }

    public static OSMEntity way(long id, int version, long time, long cs, List<Long> refs) {
        return new OSMEntity.OSMWay(id, version, Instant.ofEpochSecond(timebase + 60 * time), cs, 1, "", true, Map.of(), refs);
    }

}