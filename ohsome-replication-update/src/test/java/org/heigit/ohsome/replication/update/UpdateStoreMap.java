package org.heigit.ohsome.replication.update;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.OSMEntity.OSMRelation;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.heigit.ohsome.replication.UpdateStore;

import java.util.Map;
import java.util.Set;

import static org.heigit.ohsome.replication.UpdateStore.BackRefs.*;

public class UpdateStoreMap implements UpdateStore {
    private final LoadingCache<Long, OSMNode> nodesCache = Caffeine.newBuilder().build(k -> null);
    private final LoadingCache<Long, OSMWay> waysCache = Caffeine.newBuilder().build(k -> null);
    private final LoadingCache<Long, OSMRelation> relationsCache = Caffeine.newBuilder().build(k -> null);

    private final Map<BackRefs, LoadingCache<Long, Set<Long>>> backRefsCache = Map.of(
            NODE_WAY, Caffeine.newBuilder().build(k -> null),
            NODE_RELATION, Caffeine.newBuilder().build(k -> null),
            WAY_RELATION, Caffeine.newBuilder().build(k -> null));

    @Override
    public Map<Long, OSMNode> nodes(Set<Long> ids) {
        return nodesCache.getAll(ids);
    }

    @Override
    public void nodes(Map<Long, OSMNode> updates) {
        nodesCache.putAll(updates);
    }

    @Override
    public Map<Long, OSMWay> ways(Set<Long> ids) {
        return waysCache.getAll(ids);
    }

    @Override
    public void ways(Map<Long, OSMWay> updates) {
        waysCache.putAll(updates);
    }


    @Override
    public Map<Long, OSMRelation> relations(Set<Long> ids) {
        return relationsCache.getAll(ids);
    }

    @Override
    public void relations(Map<Long, OSMRelation> updates) {
        relationsCache.putAll(updates);
    }


    @Override
    public Map<Long, Set<Long>> backRefs(BackRefs type, Set<Long> ids) {
        return backRefsCache.get(type).getAll(ids);
    }

    @Override
    public void backRefs(BackRefs type, Map<Long, Set<Long>> updates) {
        backRefsCache.get(type).putAll(updates);
    }

    @Override
    public void close() {

    }
}
