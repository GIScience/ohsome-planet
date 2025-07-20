package org.heigit.ohsome.contributions.util;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

public class RocksMap {

    private RocksMap() {
        // utility class
    }

    public static  <T> Map<Long, T> get(RocksDB db, Set<Long> ids, BiFunction<Long, byte[], T> deserializer) {
        var keys = ids.stream()
                .map(id -> ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN).putLong(id).array())
                .toList();
        try {
            var map = new HashMap<Long, T>();

            var values = db.multiGetAsList(keys);
            for (var i = 0; i < values.size(); i++) {
                var id = ByteBuffer.wrap(keys.get(i)).order(ByteOrder.BIG_ENDIAN).getLong();
                var val = values.get(i);
                if (val == null) {
                    continue;
                }
                map.put(id, deserializer.apply(id, val));
            }
            return map;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

    }
}
