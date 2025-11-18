package org.heigit.ohsome.replication;

import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMType;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

public interface UpdateStore extends AutoCloseable {

    UpdateStore NOOP = new UpdateStore() {
        @Override
        public void close() throws Exception {

        }

        @Override
        public Map<Long, OSMEntity.OSMNode> nodes(Set<Long> ids) {
            return Map.of();
        }

        @Override
        public Map<Long, OSMEntity.OSMWay> ways(Set<Long> ids) {
            return Map.of();
        }

        @Override
        public Map<Long, Set<Long>> backRefsNodeWay(Set<Long> ids) {
            return Map.of();
        }

        @Override
        public void nodes(Map<Long, OSMEntity.OSMNode> updates) {

        }

        @Override
        public void ways(Map<Long, OSMEntity.OSMWay> updates) {

        }

        @Override
        public void backRefsNodeWay(Map<Long, Set<Long>> updates) {

        }
    };

    static UpdateStore noop() {
        return NOOP;
    }

    enum BackRefs {
        NODE_WAY,
        NODE_RELATION,
        WAY_RELATION
    }

    static Path updatePath(Path base, OSMType type) {
        return switch (type) {
            case NODE -> base.resolve("nodes");
            case WAY -> base.resolve("ways");
            case RELATION -> base.resolve("relations");
        };
    }

    static Path updatePath(Path base, BackRefs type) {
        return switch (type) {
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
