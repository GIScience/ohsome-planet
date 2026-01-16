package org.heigit.ohsome.replication;

import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.OSMEntity.OSMRelation;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;
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
        if (base == null) {
            return null;
        }
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

    Map<Long, OSMNode> nodes(Set<Long> ids);
    void nodes(Map<Long, OSMNode> updates);

    Map<Long, OSMWay> ways(Set<Long> ids);
    void ways(Map<Long, OSMWay> updates);

    Map<Long, OSMRelation> relations(Set<Long> ids);
    void relations(Map<Long, OSMRelation> updates);

    Map<Long, Set<Long>> backRefs(BackRefs type, Set<Long> ids);
    void backRefs(BackRefs type, Map<Long, Set<Long>> updates);



     UpdateStore NOOP = new UpdateStore() {

         @Override
         public void close() {

         }

         @Override
         public Map<Long, OSMNode> nodes(Set<Long> ids) {
             return Map.of();
         }

         @Override
         public void nodes(Map<Long, OSMNode> updates) {

         }

         @Override
         public Map<Long, OSMWay> ways(Set<Long> ids) {
             return Map.of();
         }

         @Override
         public void ways(Map<Long, OSMWay> updates) {

         }

         @Override
         public Map<Long, OSMRelation> relations(Set<Long> ids) {
             return Map.of();
         }

         @Override
         public void relations(Map<Long, OSMRelation> updates) {

         }

         @Override
         public Map<Long, Set<Long>> backRefs(BackRefs type, Set<Long> ids) {
             return Map.of();
         }

         @Override
         public void backRefs(BackRefs type, Map<Long, Set<Long>> updates) {

         }
     };

     static UpdateStore noop() {
         return NOOP;
     }

}
