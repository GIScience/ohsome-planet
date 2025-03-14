package org.heigit.ohsome.parquet.contrib;

import org.heigit.ohsome.contributions.contrib.Contribution;
import org.heigit.ohsome.contributions.contrib.ContributionsWay;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static java.time.Instant.ofEpochSecond;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.*;

class ContributionsWayTest {

  @Test
  @Disabled("Missing logic to pick up minor contributions based only on changes in geometry")
  void testWays() {// why do we have more contributions than one if the position of nodes doesn't change??
    var nodes = Map.of(
            1L, List.of(
                    new OSMNode(1, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), 0.0, 0.0),
                    new OSMNode(1, 2, ofEpochSecond(2), 2, 2, "", true, emptyMap(), 0.0, 0.0)
            ),
            2L, List.of(
                    new OSMNode(2, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), 0.0, 0.0),
                    new OSMNode(2, 2, ofEpochSecond(2), 2, 2, "", true, emptyMap(), 0.0, 0.0)
            )
    );
    var osh = List.of(
            new OSMWay(10, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), List.of(1L, 2L))
    );

    var contributions = new ContributionsWay(osh, nodes);

    Contribution contrib;

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(1, contrib.entity().version());
    assertEquals(1, contrib.changeset());
    assertEquals(Instant.ofEpochSecond(1), contrib.timestamp());
    assertEquals(2, contrib.members().size());


    assertFalse(contributions.hasNext());
  }

  @Test
  void testNodeVersionsBeforeWay() {
    var nodes = Map.of(
            1L, List.of(
                    new OSMNode(1L, 1, ofEpochSecond(0), 1, 1, "", true, emptyMap(), 0.0, 0.0),
                    new OSMNode(1L, 2, ofEpochSecond(1), 2, 1, "", true, emptyMap(), 0.0, 0.0)
            )
    );
    var osh = List.of(
            new OSMWay(10, 1, ofEpochSecond(2), 1, 1, "", true, emptyMap(), List.of(1L))
    );

    var contributions = new ContributionsWay(osh, nodes);
    Contribution contrib;

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(1, contrib.members().size());
    assertEquals(2, contrib.members().getFirst().contrib().entity().version());

    assertFalse(contributions.hasNext());
  }

  @Test
  void testWayExistsBeforeNode() {
    var nodes = Map.of(
            1L, List.of(
                    new OSMNode(1L, 1, ofEpochSecond(2), 1, 1, "", true, emptyMap(), 1.0, 0.0)
            )
    );

    var osh = List.of(
            new OSMWay(10, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), List.of(1L))
    );

    var contributions = new ContributionsWay(osh, nodes);
    Contribution contrib;

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(1, contrib.entity().version()); // minorVersion = 0
    assertEquals(1, contrib.members().size());
    assertEquals(1, contrib.members().getFirst().id());

    assertFalse(contributions.hasNext());
  }

  @Test
  void testNodeVersionsBeforePlusMinorVersionWay() {
    var nodes = Map.of(
            1L, List.of(
                    new OSMNode(1L, 1, ofEpochSecond(0), 1, 1, "", true, emptyMap(), 1.0, 0.0),
                    new OSMNode(1L, 2, ofEpochSecond(1), 2, 1, "", true, emptyMap(), 2.0, 0.0),
                    new OSMNode(1L, 3, ofEpochSecond(5), 5, 1, "", true, emptyMap(), 3.0, 0.0),
                    new OSMNode(1L, 4, ofEpochSecond(6), 6, 1, "", true, emptyMap(), 4.0, 0.0)
            )
    );
    var osh = List.of(
            new OSMWay(10, 1, ofEpochSecond(2), 1, 1, "", true, emptyMap(), List.of(1L)),
            new OSMWay(10, 2, ofEpochSecond(7), 1, 1, "", true, emptyMap(), List.of(1L))
    );

    var contributions = new ContributionsWay(osh, nodes);
    Contribution contrib;

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(1, contrib.entity().version()); // minorVersion = 0
    assertEquals(1, contrib.members().size());
    assertEquals(2, contrib.members().getFirst().contrib().entity().version());

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(1, contrib.entity().version()); // minorVersion = 1
    assertEquals(1, contrib.members().size());
    assertEquals(3, contrib.members().getFirst().contrib().entity().version());

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(1, contrib.entity().version()); // minorVersion = 1
    assertEquals(1, contrib.members().size());
    assertEquals(4, contrib.members().getFirst().contrib().entity().version());

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(2, contrib.entity().version()); // minorVersion = 1
    assertEquals(1, contrib.members().size());
    assertEquals(4, contrib.members().getFirst().contrib().entity().version());

    assertFalse(contributions.hasNext());
  }

  @Test
  void testTwoNodesMovedInDifferentChangesets() {
    var nodes = Map.of(
            1L, List.of(
                    new OSMNode(1L, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), 1.0, 0.0),
                    new OSMNode(1L, 2, ofEpochSecond(2), 2, 2, "", true, emptyMap(), 1.1, 0.0)
            ),
            2L, List.of(
                    new OSMNode(2L, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), 2.0, 0.0),
                    new OSMNode(2L, 2, ofEpochSecond(2), 3, 3, "", true, emptyMap(), 2.2, 0.0)
            )

    );
    var osh = List.of(
            new OSMWay(12, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), List.of(1L, 2L))
    );

    var contributions = new ContributionsWay(osh, nodes);
    Contribution contrib;

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(1, contrib.changeset());
    assertEquals(ofEpochSecond(1), contrib.timestamp());
    assertEquals(1, contrib.entity().version());// minorVersion = 0
    assertEquals(2, contrib.members().size());
    assertEquals(1, contrib.members().getFirst().contrib().entity().version());

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(2, contrib.changeset());
    assertEquals(ofEpochSecond(2), contrib.timestamp());
    assertEquals(1, contrib.entity().version());// minorVersion = 1
    assertEquals(2, contrib.members().size());
    assertEquals(2, contrib.members().getFirst().contrib().entity().version());
    assertEquals(1, contrib.members().getLast().contrib().entity().version());

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(3, contrib.changeset());
    assertEquals(ofEpochSecond(2), contrib.timestamp());
    assertEquals(1, contrib.entity().version());// minorVersion = 2
    assertEquals(2, contrib.members().size());
    assertEquals(2, contrib.members().getFirst().contrib().entity().version());
    assertEquals(2, contrib.members().getLast().contrib().entity().version());

    assertFalse(contributions.hasNext());
  }
  @Test
  void testTwoNodesMovedInDifferentChangesetsAtDifferentTimepoints() {
    var nodes = Map.of(
            1L, List.of(
                    new OSMNode(1L, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), 1.0, 0.0),
                    new OSMNode(1L, 2, ofEpochSecond(2), 2, 2, "", true, emptyMap(), 1.1, 0.0)
            ),
            2L, List.of(
                    new OSMNode(2L, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), 2.0, 0.0),
                    new OSMNode(2L, 2, ofEpochSecond(3), 3, 3, "", true, emptyMap(), 2.2, 0.0)
            )

    );
    var osh = List.of(
            new OSMWay(12, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), List.of(1L, 2L))
    );

    var contributions = new ContributionsWay(osh, nodes);
    Contribution contrib;

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(1, contrib.changeset());
    assertEquals(ofEpochSecond(1), contrib.timestamp());
    assertEquals(1, contrib.entity().version());// minorVersion = 0
    assertEquals(2, contrib.members().size());
    assertEquals(1, contrib.members().getFirst().contrib().entity().version());

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(2, contrib.changeset());
    assertEquals(ofEpochSecond(2), contrib.timestamp());
    assertEquals(1, contrib.entity().version());// minorVersion = 1
    assertEquals(2, contrib.members().size());
    assertEquals(2, contrib.members().getFirst().contrib().entity().version());
    assertEquals(1, contrib.members().getLast().contrib().entity().version());

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(3, contrib.changeset());
    assertEquals(ofEpochSecond(3), contrib.timestamp());
    assertEquals(1, contrib.entity().version());// minorVersion = 2
    assertEquals(2, contrib.members().size());
    assertEquals(2, contrib.members().getFirst().contrib().entity().version());
    assertEquals(2, contrib.members().getLast().contrib().entity().version());

    assertFalse(contributions.hasNext());
  }

  @Test
  void testNodeMovedMultipleTimesWithinOneChangeset() {
    var nodes = Map.of(
            1L, List.of(
                    new OSMNode(1L, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), 1.0, 0.0),
                    new OSMNode(1L, 2, ofEpochSecond(2), 2, 2, "", true, emptyMap(), 2.0, 0.0),
                    new OSMNode(1L, 3, ofEpochSecond(3), 2, 2, "", true, emptyMap(), 3.0, 0.0)
            )
    );
    var osh = List.of(
            new OSMWay(10, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), List.of(1L))
    );

    var contributions = new ContributionsWay(osh, nodes);
    Contribution contrib;

    assertTrue(contributions.hasNext());//minor version 0
    contrib = contributions.next();
    assertEquals(1, contrib.changeset());
    assertEquals(ofEpochSecond(1), contrib.timestamp());

    assertTrue(contributions.hasNext());//minor version 1
    contrib = contributions.next();
    assertEquals(2, contrib.changeset());
    assertEquals(ofEpochSecond(3), contrib.timestamp());

    assertFalse(contributions.hasNext());
  }

  @Test
  @Disabled("Missing logic to discard geometry-unrelated edits to node")
  void testNodeTagsModifiedMultipleTimesWithinOneChangeset() {
    var nodes = Map.of(
            1L, List.of(
                    new OSMNode(1L, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), 1.0, 0.0),
                    new OSMNode(1L, 2, ofEpochSecond(2), 2, 2, "", true, emptyMap(), 1.0, 0.0),
                    new OSMNode(1L, 3, ofEpochSecond(3), 2, 2, "", true, emptyMap(), 1.0, 0.0)
            )
    );
    var osh = List.of(
            new OSMWay(10, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), List.of(1L))
    );

    var contributions = new ContributionsWay(osh, nodes);
    Contribution contrib;

    assertTrue(contributions.hasNext());//minor version 0
    contrib = contributions.next();
    assertEquals(1, contrib.changeset());
    assertEquals(ofEpochSecond(1), contrib.timestamp());

    assertFalse(contributions.hasNext());
  }

  @Test
  void testTwoNodesMovedWithinOneChangeset() {
    var nodes = Map.of(
            1L, List.of(
                    new OSMNode(1L, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), 1.0, 0.0),
                    new OSMNode(1L, 2, ofEpochSecond(2), 2, 2, "", true, emptyMap(), 1.1, 0.0)
            ),
            2L, List.of(
                    new OSMNode(2L, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), 2.0, 0.0),
                    new OSMNode(2L, 2, ofEpochSecond(2), 2, 2, "", true, emptyMap(), 2.2, 0.0)
            )
    );
    var osh = List.of(
            new OSMWay(12, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), List.of(1L, 2L))
    );

    var contributions = new ContributionsWay(osh, nodes);
    Contribution contrib;

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(1, contrib.changeset());

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(2, contrib.changeset());

    assertFalse(contributions.hasNext());
  }

  @Test
  void testTwoNodesMovedAtDifferentTimepointsWithinOneChangeset() {
    var nodes = Map.of(
            1L, List.of(
                    new OSMNode(1L, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), 1.0, 0.0),
                    new OSMNode(1L, 2, ofEpochSecond(2), 2, 2, "", true, emptyMap(), 1.1, 0.0)
            ),
            2L, List.of(
                    new OSMNode(2L, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), 2.0, 0.0),
                    new OSMNode(2L, 2, ofEpochSecond(3), 2, 2, "", true, emptyMap(), 2.2, 0.0)
            )
    );
    var osh = List.of(
            new OSMWay(12, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), List.of(1L, 2L))
    );

    var contributions = new ContributionsWay(osh, nodes);
    Contribution contrib;

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(1, contrib.changeset());
    assertEquals(ofEpochSecond(1), contrib.timestamp());

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(2, contrib.changeset());
    assertEquals(ofEpochSecond(3), contrib.timestamp());

    assertFalse(contributions.hasNext());
  }

  @Test
  void testMultipleNodesMovedInReverseOrderWithinOneChangeset() {
    var nodes = Map.of(
            1L, List.of(
                    new OSMNode(1L, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), 1.0, 0.0),
                    new OSMNode(1L, 2, ofEpochSecond(3), 2, 2, "", true, emptyMap(), 1.1, 0.0)
            ),
            2L, List.of(
                    new OSMNode(2L, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), 2.0, 0.0),
                    new OSMNode(2L, 2, ofEpochSecond(2), 2, 2, "", true, emptyMap(), 2.2, 0.0)
            )
    );
    var osh = List.of(
            new OSMWay(12, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), List.of(1L, 2L))
    );

    var contributions = new ContributionsWay(osh, nodes);
    Contribution contrib;

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(1, contrib.changeset());
    assertEquals(ofEpochSecond(1), contrib.timestamp());

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(2, contrib.changeset());
    assertEquals(ofEpochSecond(3), contrib.timestamp());

    assertFalse(contributions.hasNext());
  }

  @Test
  void testReferencedNodeHasOlderTimestampThanWay() {// why do we have more contributions than one if the position of nodes doesn't change??
    var nodes = Map.of(
            1L, List.of(
                    new OSMNode(1, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), 1.0, 0.0)
            ),
            2L, List.of(
                    new OSMNode(2, 1, ofEpochSecond(3), 1, 1, "", true, emptyMap(), 2.0, 0.0)
            )
    );
    var osh = List.of(
            new OSMWay(10, 1, ofEpochSecond(2), 1, 1, "", true, emptyMap(), List.of(1L, 2L))
    );

    var contributions = new ContributionsWay(osh, nodes);

    Contribution contrib;

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(1, contrib.entity().version());
    assertEquals(1, contrib.changeset());
    assertEquals(Instant.ofEpochSecond(2), contrib.timestamp());
    assertEquals(2, contrib.members().size());


    assertFalse(contributions.hasNext());
  }


}
