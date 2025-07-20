package org.heigit.ohsome.contributions.minor;

import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.contributions.util.RocksMap;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface MinorNodeStorage extends AutoCloseable {

    static MinorNodeStorage inRocksMap(Path path) throws RocksDBException, IOException {
        final var db = new RocksMap(path);
        return new MinorNodeStorage() {
            @Override
            public Map<Long, List<OSMNode>> getNodes(Set<Long> nodes) {
                return db.get(nodes, MinorNode::deserialize);
            }

            @Override
            public void close() {
                db.close();
            }
        };
    }


    Map<Long, List<OSMNode>> getNodes(Set<Long> nodes);
}
