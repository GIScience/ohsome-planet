package org.heigit.ohsome.replication.update;

import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMType;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

public interface UpdateStore extends AutoCloseable {

    enum BackRefs {
        NODE_WAY,
        NODE_RELATION,
        WAY_RELATION
    }

    static Path updatePath(Path base, OSMType type) {
        return switch(type) {
            case NODE -> base.resolve("nodes");
            case WAY -> base.resolve("ways");
            case RELATION -> base.resolve("relations");
        };
    }

    static Path updatePath(Path base, BackRefs type) {
        return switch(type) {
            case NODE_WAY -> base.resolve("node_ways");
            case NODE_RELATION -> base.resolve("node_relations");
            case WAY_RELATION -> base.resolve("way_relations");
        };
    }

    Map<Long, OSMEntity.OSMNode> nodes(Set<Long> ids);

    Map<Long, OSMEntity.OSMWay> ways(Set<Long> ids);

    Map<Long, Set<Long>> backRefsNodeWay(Set<Long> ids);

    void nodes(Map<Long, OSMEntity.OSMNode> updates);

    void ways(Map<Long, OSMEntity.OSMWay> updates);

    void backRefsNodeWay(Map<Long, Set<Long>> updates);
}
