package org.heigit.ohsome.replication.update;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.io.WKBReader;

class ContributionUpdaterTest {

  private static final long timeBase = Instant.parse("2025-10-01T00:00:00Z").getEpochSecond();
  private static final WKBReader wkb = new WKBReader();


  @Test
  void bla() {
    var store = new UpdaterStore();
    var updater = new ContributionUpdater(store);

    updater.update(List.of(
        node(1, 1, 1),
        node(2, 1, 1),
        way(1, 1, 1, List.of(1L, 2L))
    ).iterator());
    System.out.println();
    updater.update(List.of(
        node(1, 1, 1),
        node(2, 1, 1),
        way(1, 1, 1, List.of(1L, 2L))
    ).iterator());
    System.out.println();
    updater.update(List.of(
        node(1, 2, 2),
        way(1, 2, 2, List.of(1L, 2L))
    ).iterator());


  }

  private static OSMEntity node(long id, int version, long time) {
    return new OSMNode(id, version, Instant.ofEpochSecond(timeBase + 60 * time), 1L, 1, "", true,
        Map.of(), id, version);
  }


  private static OSMEntity way(long id, int version, long time, List<Long> refs) {
    return new OSMWay(id, version, Instant.ofEpochSecond(timeBase + 60 * time), 1L, 1, "", true,
        Map.of(), refs);
  }

}