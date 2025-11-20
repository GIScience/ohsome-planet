package org.heigit.ohsome.replication.update;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.heigit.ohsome.replication.UpdateStore;

import java.util.Map;
import java.util.Set;

public class UpdateStoreMap implements UpdateStore {
    private final LoadingCache<Long, OSMNode> nodesCache = Caffeine.newBuilder().build(k -> null);
    private final LoadingCache<Long, OSMWay> waysCache = Caffeine.newBuilder().build(k -> null);
    private final LoadingCache<Long, Set<Long>> nodeWayBackRefsCache = Caffeine.newBuilder().build(k -> null);

    @Override
    public Map<Long, OSMNode> nodes(Set<Long> ids) {
        return nodesCache.getAll(ids);
    }

    @Override
    public Map<Long, OSMWay> ways(Set<Long> ids) {
        return waysCache.getAll(ids);
    }

    @Override
    public Map<Long, Set<Long>> backRefsNodeWay(Set<Long> ids) {
        return nodeWayBackRefsCache.getAll(ids);
    }

    @Override
    public void nodes(Map<Long, OSMNode> updates) {
        nodesCache.putAll(updates);
    }

    @Override
    public void ways(Map<Long, OSMWay> updates) {
        waysCache.putAll(updates);
    }

    @Override
    public void backRefsNodeWay(Map<Long, Set<Long>> updates) {
        nodeWayBackRefsCache.putAll(updates);
    }

    @Override
    public void close() throws Exception {

    }
}
