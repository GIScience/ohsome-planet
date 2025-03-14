package org.heigit.ohsome.contributions.minor;

import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.heigit.ohsome.contributions.util.RocksMap;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface MinorWayStorage extends AutoCloseable{

    static MinorWayStorage inRocksMap(Path path) throws RocksDBException, IOException {
        return new MinorWayStorage() {
            final RocksMap db = new RocksMap(path);
            @Override
            public Map<Long, List<OSMWay>> getWays(Set<Long> ways) {
                return db.get(ways, MinorWay::deserialize);
            }

            @Override
            public void close() throws Exception {
                db.close();
            }
        };
    }


    Map<Long, List<OSMWay>> getWays(Set<Long> ways);
}
