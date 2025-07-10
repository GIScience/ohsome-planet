package org.heigit.ohsome.contributions.contrib;

import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static java.time.Instant.ofEpochSecond;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.*;

class ContributionsNodeTest {

  @Test
  void testNodes() {
    var node = List.of(
            new OSMNode(1, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), 0.0, 0.0),
            new OSMNode(1, 2, ofEpochSecond(2), 2, 2, "", true, emptyMap(), 0.0, 0.0)
    );
    var contributions = new ContributionsNode(node);

    Contribution contrib;

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(1, contrib.entity().version());

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(2, contrib.entity().version());

    assertFalse(contributions.hasNext());
  }

}
