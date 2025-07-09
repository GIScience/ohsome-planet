package org.heigit.ohsome.contributions.util;

import org.apache.commons.lang3.SerializationUtils;
import org.heigit.ohsome.contributions.minor.MinorNode;
import org.heigit.ohsome.contributions.rocksdb.RocksUtil;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

public class RocksMap implements AutoCloseable {

    private final RocksDB db;

    public RocksMap(Path path) throws RocksDBException, IOException {
        Files.createDirectories(path.getParent());
        var options = RocksUtil.defaultOptions().setCreateIfMissing(true);
        this.db = RocksDB.open(options, path.toString());
    }

    public MinorNode get(byte[] key) {
        try {
            byte[] value = db.get(key);
            if (value != null) {
                return SerializationUtils.deserialize(value);
            }
            return null;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void close() {
        db.close();
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


    public <T> Map<Long, T> get(Set<Long> nodes, BiFunction<Long, byte[], T> deserializer) {
        return get(db, nodes, deserializer);
    }
}
