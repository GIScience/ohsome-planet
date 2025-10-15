package org.heigit.ohsome.replication.update;

import static com.google.common.collect.Iterators.filter;
import static com.google.common.collect.Iterators.transform;

import com.google.common.collect.Iterators;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.heigit.ohsome.contributions.avro.Contrib;
import org.heigit.ohsome.contributions.avro.ContribChangeset;
import org.heigit.ohsome.contributions.contrib.Contribution;
import org.heigit.ohsome.contributions.contrib.ContributionsAvroConverter;
import org.heigit.ohsome.contributions.contrib.ContributionsNode;
import org.heigit.ohsome.contributions.contrib.ContributionsWay;
import org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.heigit.ohsome.osm.OSMType;

public class ContributionUpdater {

  public record Node(List<OSMNode> newVersions, OSMNode before) {

    public List<OSMNode> osh() {
      var osh = new ArrayList<OSMNode>(newVersions.size() + 1);
      if (before != null) {
        osh.add(before);
      }
      osh.addAll(newVersions);
      return osh;
    }

  }

  public record Way(List<OSMWay> newVersions, OSMWay before) {

  }


  private final SpatialJoiner countryJoiner = SpatialJoiner.noop();
  private final UpdaterStore store;


  private Map<Long, Node> oscNodes;
  private Map<Long, Way> oscWays;

  public ContributionUpdater(UpdaterStore store) {
    this.store = store;
  }

  public void update(Iterator<OSMEntity> osc) {
    osc = Iterators.peekingIterator(osc);
    oscNodes = oscNodes(osc);
    oscNodes.forEach((id, node) -> {
      var itr = updateNode(node);
      while (itr.hasNext()) {
        System.out.println("node " + id + " = " + itr.next());
      }
    });


    oscWays = oscWays(osc);
    oscWays.forEach((id, way) -> {
      var itr = updateWay(way);
      while (itr.hasNext()) {
        System.out.println("way " + id + " = " + itr.next());
      }
    });



    var updateNodes = new HashMap<Long, List<OSMNode>>();
    oscNodes.forEach((id, node) -> updateNodes.put(id, List.of(node.newVersions().getLast())));
    store.updateNodes(updateNodes);
    var updateWays = new HashMap<Long, List<OSMWay>>();
    oscWays.forEach((id, way) -> updateWays.put(id, List.of(way.newVersions().getLast())));
    store.updateWays(updateWays);

    oscWays.forEach((id, way) -> updateNodeWayBackRef(way));

    Map<Long, Set<Long>> backRefsToExist = new HashMap<>();
    Map<Long, Set<Long>> backRefsToRemove = new HashMap<>();
    oscWays.forEach((id, way) -> {
      var nodeIds = new HashSet<Long>();
      if (!way.newVersions().isEmpty()) {
        nodeIds.addAll(way.newVersions().getLast().refs());
      }
      if (way.before() != null) {
        way.before().refs().forEach(ref -> {
          if (!nodeIds.contains(ref)) {
            backRefsToRemove.computeIfAbsent(ref, x -> new HashSet<>()).add(ref);
          }
        });
      }
      nodeIds.forEach(ref -> backRefsToExist.computeIfAbsent(ref, x -> new HashSet<>()).add(ref));
    });

    store.updateNodeWayBackRef(backRefsToExist, backRefsToRemove);
  }

  private void updateNodeWayBackRef(Way way) {
    var nodeIds = new HashSet<Long>();
    var removeBackRef = new HashSet<Long>();
    if (!way.newVersions().isEmpty()) {
      nodeIds.addAll(way.newVersions().getLast().refs());
    }
    if (way.before() != null) {
      way.before().refs().forEach(ref -> {
        if (!nodeIds.contains(ref)) {
          removeBackRef.add(ref);
        }
      });
    }
  }

  public Map<Long, Node> oscNodes(Iterator<OSMEntity> osc) {
    var itr = Iterators.peekingIterator(osc);
    var nodes = new HashMap<Long, List<OSMNode>>();
    while (itr.hasNext() && itr.peek().type() == OSMType.NODE) {
      var id = itr.peek().id();
      var osh = new ArrayList<OSMNode>();
      while (itr.hasNext() && itr.peek().type() == OSMType.NODE && itr.peek().id() == id) {
        osh.add((OSMNode) itr.next());
      }
      nodes.put(id, osh);
    }
    var nodesBefore = store.getNodes(nodes.keySet());
    var oscNodes = new HashMap<Long, Node>();
    nodes.forEach((id, osh) -> {
      var before = nodesBefore.get(id);
      if (before != null) {
        osh.removeIf(osm -> osm.version() <= before.getLast().version());
      }
      if (!osh.isEmpty()) {
        oscNodes.put(id, new Node(osh, before != null ? before.getLast() : null));
      }
    });
    return oscNodes;
  }

  public Map<Long, Way> oscWays(Iterator<OSMEntity> osc) {
    var itr = Iterators.peekingIterator(osc);
    var entities = new HashMap<Long, List<OSMWay>>();
    while (itr.hasNext() && itr.peek().type() == OSMType.WAY) {
      var id = itr.peek().id();
      var osh = new ArrayList<OSMWay>();
      while (itr.hasNext() && itr.peek().type() == OSMType.WAY && itr.peek().id() == id) {
        osh.add((OSMWay) itr.next());
      }
      entities.put(id, osh);
    }
    var entitiesBefore = store.getWays(entities.keySet());
    var oscEntity = new HashMap<Long, Way>();
    entities.forEach((id, osh) -> {
      var before = entitiesBefore.get(id);
      if (before != null) {
        osh.removeIf(osm -> osm.version() <= before.getLast().version());
      }
      if (!osh.isEmpty()) {
        oscEntity.put(id, new Way(osh, before != null ? before.getLast() : null));
      }
    });
    return oscEntity;
  }


  public Iterator<Contrib> updateNode(Node node) {
    var contributions = new ContributionsNode(node.osh());
    if (contributions.hasNext() && node.before() != null) {
      contributions.next();
    }
    var converter = new ContributionsAvroConverter(contributions, this::changeset, countryJoiner);
    return transform(filter(converter, Optional::isPresent), Optional::orElseThrow);
  }

  public Iterator<Contrib> updateWay(Way way) {

    var refIds = new HashSet<Long>();
    way.newVersions().forEach(osm -> refIds.addAll(osm.refs()));
    if (way.before() != null) {
      refIds.addAll(way.before().refs());
    }

    var nodes = store.getNodes(refIds);
    Contribution contribBefore = null;
    if (way.before() != null) {
      var contributions = new ContributionsWay(List.of(way.before()), nodes);
      contribBefore = contributions.next();
    }

    var osh = new ArrayList<OSMWay>(way.newVersions().size() + 1);
    if (way.before() != null) {
      osh.add(way.before());
    }
    osh.addAll(way.newVersions());

    // update nodes with osc nodes
    nodes.forEach((id, node) -> {
      var osc = oscNodes.get(id);
      if (osc != null) {
        nodes.put(id, osc.osh());
      }
    });

    var contributions = new ContributionsWay(osh, nodes);
    if (contribBefore != null && contributions.hasNext() && contribBefore.equals(
        contributions.peek())) {
      contributions.next();
    }
    var converter = new ContributionsAvroConverter(contributions, this::changeset, countryJoiner);
    return transform(filter(converter, Optional::isPresent), Optional::orElseThrow);
  }

  private ContribChangeset changeset(long id) {
    var changesetBuilder = ContribChangeset.newBuilder();
    changesetBuilder
        .setCreatedAt(Instant.ofEpochSecond(0))
        .clearClosedAt().clearTags().clearHashtags().clearEditor().clearNumChanges();
    return changesetBuilder.setId(id).build();
  }

}
