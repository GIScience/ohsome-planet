package org.heigit.ohsome.replication.update;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;

public class UpdaterStore {
  private final LoadingCache<Long, List<OSMNode>> nodes = Caffeine.newBuilder().build(k -> null);
  private final LoadingCache<Long, List<OSMWay>> ways = Caffeine.newBuilder().build(k -> null);

  private final LoadingCache<Long, Set<Long>> nodeWayBackRefs = Caffeine.newBuilder().build(k -> null);

  public Map<Long, List<OSMNode>> getNodes(Set<Long> ids) {
    return new HashMap<Long, List<OSMNode>>(nodes.getAll(ids));
  }

  public Map<Long, List<OSMWay>> getWays(Set<Long> ids) {
    return ways.getAll(ids);
  }

  public void updateNodes(Map<Long, List<OSMNode>> updates) {
    nodes.putAll(updates);
  }

  public void updateWays(Map<Long, List<OSMWay>> updates) {
    ways.putAll(updates);
  }


  public Map<Long, Set<Long>> getNodeWayBackRef(Set<Long> nodeIds) {
        return nodeWayBackRefs.getAll(nodeIds);
  }

  public void updateNodeWayBackRef(Map<Long, Set<Long>> backRefsToExist, Map<Long, Set<Long>> backRefsToRemove) {
    var allIds = new HashSet<Long>();
    allIds.addAll(backRefsToExist.keySet());
    allIds.addAll(backRefsToRemove.keySet());

    var backRefs = nodeWayBackRefs.getAll(allIds);
    backRefs.forEach((id, refs) -> {
      refs.addAll(backRefsToExist.getOrDefault(id, Set.of()));
      refs.removeAll(backRefsToRemove.getOrDefault(id, Set.of()));
    });

    nodeWayBackRefs.putAll(backRefs);
  }


}
