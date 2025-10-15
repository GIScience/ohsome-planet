package org.heigit.ohsome.replication.update;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;

import java.util.Map;
import java.util.Set;

public class UpdateStore {
    private final LoadingCache<Long, OSMNode> nodesCache = Caffeine.newBuilder().build(k -> null);
    private final LoadingCache<Long, OSMWay> waysCache = Caffeine.newBuilder().build(k -> null);
    private final LoadingCache<Long, Set<Long>> nodeWayBackRefsCache = Caffeine.newBuilder().build(k -> null);

    public Map<Long, OSMNode> getNodes(Set<Long> ids) {
        return nodesCache.getAll(ids);
    }

    public Map<Long, OSMWay> getWays(Set<Long> ids) {
        return waysCache.getAll(ids);
    }

    public Map<Long, Set<Long>> getNodeWayBackRefs(Set<Long> ids) {
        return nodeWayBackRefsCache.getAll(ids);
    }

    public void updateNodes(Map<Long, OSMNode> updates) {
        nodesCache.putAll(updates);
    }

    public void updateWays(Map<Long, OSMWay> updates) {
        waysCache.putAll(updates);
    }

    public void updateNodeWayBackRefs(Map<Long, Set<Long>> updates) {
        nodeWayBackRefsCache.putAll(updates);
    }
}
