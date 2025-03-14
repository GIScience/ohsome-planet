package org.heigit.ohsome.parquet.contrib;

import org.heigit.ohsome.contributions.contrib.Contribution;
import org.heigit.ohsome.contributions.contrib.Contributions;
import org.heigit.ohsome.contributions.contrib.ContributionsNode;
import org.heigit.ohsome.contributions.contrib.ContributionsRelation;
import org.heigit.ohsome.contributions.contrib.ContributionsWay;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.OSMEntity.OSMRelation;
import org.heigit.ohsome.osm.OSMId;
import org.heigit.ohsome.osm.OSMMember;
import org.heigit.ohsome.osm.OSMType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static java.time.Instant.ofEpochSecond;
import static java.util.Collections.emptyMap;
import static org.heigit.ohsome.osm.OSMEntity.*;
import static org.junit.jupiter.api.Assertions.*;

class ContributionsRelationTest {
  private static final List<OSMNode> contributionsListNodeA = List.of(
          new OSMNode(1, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), 0.0, 0.0),
          new OSMNode(1, 2, ofEpochSecond(2), 2, 2, "", true, emptyMap(), 1.0, 0.0)
          );
  private Contributions contributionsNodeA;

  private static final List<OSMNode> contributionsListNodeB = List.of(
          new OSMNode(2, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), 0.0, 0.5),
          new OSMNode(2, 2, ofEpochSecond(2), 2, 2, "", true, emptyMap(), 0.0, 1.0)
  );
  private Contributions contributionsNodeB;

  private static final List<OSMNode> contributionsListNodeC = List.of(
          new OSMNode(3, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), 0.0, 2.0)
  );
  private Contributions contributionsNodeC;

  private static final Map<Long, List<OSMNode>> nodes = Map.of(
          1L, contributionsListNodeA,
          2L, contributionsListNodeB,
          3L, contributionsListNodeC
  );

  private static final List<OSMWay> contributionsListWayAB = List.of(new OSMWay(12, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), List.of(1L, 2L)));
  private Contributions contributionsWayAB;

  private static final List<OSMWay> contributionsListWayBC = List.of(new OSMWay(23, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), List.of(2L, 3L)));
  private Contributions contributionsWayBC;

  private static final List<OSMWay> contributionsListWayCA = List.of(
          new OSMWay(31, 1, ofEpochSecond(2), 2, 2, "", true, emptyMap(), List.of(3L, 1L)),
          new OSMWay(31, 2, ofEpochSecond(3), 3, 3, "", true, emptyMap(), List.of(3L, 1L)));
  private Contributions contributionsWayCA;


  @BeforeEach
  void initContributions() {
    contributionsNodeA = new ContributionsNode(contributionsListNodeA);
    contributionsNodeB = new ContributionsNode(contributionsListNodeB);
    contributionsNodeC = new ContributionsNode(contributionsListNodeC);
    contributionsWayAB = new ContributionsWay(contributionsListWayAB, nodes);
    contributionsWayBC = new ContributionsWay(contributionsListWayBC, nodes);
    contributionsWayCA = new ContributionsWay(contributionsListWayCA, nodes);
  }

  @Test
  void testSingleNodeRelation() {
    var members = Map.of(new OSMId(OSMType.NODE, 1L), contributionsNodeA);
    List<OSMMember> membersList = List.of(new OSMMember(OSMType.NODE, 1L, "busstop"));

    var osh = List.of(
            new OSMRelation(1, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), membersList)
    );

    var contributions = new ContributionsRelation(osh, members);

    Contribution contrib;

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(1, contrib.members().size());
    assertEquals(1, contrib.entity().version());// minor version 0
    assertEquals(1, contrib.changeset());
    assertEquals(Instant.ofEpochSecond(1), contrib.timestamp());

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(1, contrib.entity().version());// minor version 1
    assertEquals(2, contrib.changeset());
    assertEquals(Instant.ofEpochSecond(2), contrib.timestamp());

    assertFalse(contributions.hasNext());
  }

  @Test
  void testTwoNodesRelation() {
    var members = Map.of(
            new OSMId(OSMType.NODE, 2L), contributionsNodeB,
            new OSMId(OSMType.NODE, 3L), contributionsNodeC
    );

    List<OSMMember> membersList = members.keySet().stream().map(m -> new OSMMember(m.type(), m.id(), "busstop")).toList();

    var osh = List.of(
            new OSMRelation(23, 1, ofEpochSecond(2), 2, 2, "", true, emptyMap(), membersList)
    );

    var contributions = new ContributionsRelation(osh, members);

    Contribution contrib;

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(2, contrib.members().size());
    assertEquals(1, contrib.entity().version());
    assertEquals(2, contrib.changeset());
    assertEquals(Instant.ofEpochSecond(2), contrib.timestamp());

    assertFalse(contributions.hasNext());
  }

  @Test
  void testTwoWaysRelation() {
    var members = Map.of(
            new OSMId(OSMType.WAY, 12), contributionsWayAB,
            new OSMId(OSMType.WAY, 23), contributionsWayBC
    );

    List<OSMMember> membersList = members.keySet().stream().map(m -> new OSMMember(m.type(), m.id(), "busline")).toList();

    var osh = List.of(
            new OSMRelation(123, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), membersList)
    );

    var contributions = new ContributionsRelation(osh, members);

    Contribution contrib;

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(2, contrib.members().size());
    assertEquals(1, contrib.entity().version());// minor version 0
    assertEquals(1, contrib.changeset());
    assertEquals(Instant.ofEpochSecond(1), contrib.timestamp());

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(1, contrib.entity().version());// minor version 1
    assertEquals(2, contrib.changeset());
    assertEquals(Instant.ofEpochSecond(2), contrib.timestamp());

    assertFalse(contributions.hasNext());
  }

  @Test
  void testMinorVersions() {
    var members = Map.of(
            new OSMId(OSMType.WAY, 12), contributionsWayAB,
            new OSMId(OSMType.WAY, 23), contributionsWayBC,
            new OSMId(OSMType.WAY, 31), contributionsWayCA
    );

    List<OSMMember> membersList = members.keySet().stream().map(m -> new OSMMember(m.type(), m.id(), "busline")).toList();

    var osh = List.of(
            new OSMRelation(123, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), membersList)
    );

    var contributions = new ContributionsRelation(osh, members);

    Contribution contrib;

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(3, contrib.members().size());
    assertEquals(1, contrib.entity().version());// minor version 0
    assertEquals(1, contrib.changeset());
    assertEquals(Instant.ofEpochSecond(1), contrib.timestamp());

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(1, contrib.entity().version());// minor version 1 due to changes to nodes
    assertEquals(2, contrib.changeset());
    assertEquals(Instant.ofEpochSecond(2), contrib.timestamp());

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(1, contrib.entity().version());// minor version 2 due to changes to ways
    assertEquals(3, contrib.changeset());
    assertEquals(Instant.ofEpochSecond(3), contrib.timestamp());

    assertFalse(contributions.hasNext());
  }

  @Test
  void testMajorVersionsWithDifferentMembers() {
    var members = Map.of(
            new OSMId(OSMType.WAY, 12), contributionsWayAB,
            new OSMId(OSMType.WAY, 23), contributionsWayBC,
            new OSMId(OSMType.WAY, 31), contributionsWayCA
    );

    List<OSMMember> membersList = members.keySet().stream().map(m -> new OSMMember(m.type(), m.id(), "busline")).toList();

    var osh = List.of(
            new OSMRelation(123, 1, ofEpochSecond(2), 2, 1, "", true, emptyMap(), membersList),
            new OSMRelation(123, 2, ofEpochSecond(3), 3, 2, "", true, emptyMap(), membersList.subList(0,2))
    );

    var contributions = new ContributionsRelation(osh, members);

    Contribution contrib;

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(3, contrib.members().size());
    assertEquals(1, contrib.entity().version());
    assertEquals(2, contrib.changeset());
    assertEquals(Instant.ofEpochSecond(2), contrib.timestamp());

    assertTrue(contributions.hasNext());
    contrib = contributions.next();
    assertEquals(2, contrib.members().size());
    assertEquals(2, contrib.entity().version());
    assertEquals(3, contrib.changeset());
    assertEquals(Instant.ofEpochSecond(3), contrib.timestamp());

    assertFalse(contributions.hasNext());
  }

}
