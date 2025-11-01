package org.heigit.ohsome.replication.update;

import org.heigit.ohsome.osm.OSMEntity;

import java.util.Map;
import java.util.Set;

public interface UpdateStore {
    Map<Long, OSMEntity.OSMNode> nodes(Set<Long> ids);

    Map<Long, OSMEntity.OSMWay> ways(Set<Long> ids);

    Map<Long, Set<Long>> backRefsNodeWay(Set<Long> ids);

    void nodes(Map<Long, OSMEntity.OSMNode> updates);

    void ways(Map<Long, OSMEntity.OSMWay> updates);

    void backRefsNodeWay(Map<Long, Set<Long>> updates);
}
