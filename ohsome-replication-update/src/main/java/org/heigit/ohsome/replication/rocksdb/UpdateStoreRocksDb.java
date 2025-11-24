package org.heigit.ohsome.replication.rocksdb;

import com.google.common.collect.Maps;
import org.heigit.ohsome.contributions.rocksdb.RocksUtil;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.OSMEntity.OSMRelation;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.heigit.ohsome.osm.OSMType;
import org.heigit.ohsome.replication.ReplicationEntity;
import org.heigit.ohsome.replication.UpdateStore;
import org.heigit.ohsome.util.io.Output;
import org.rocksdb.*;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static org.heigit.ohsome.contributions.rocksdb.RocksUtil.defaultOptions;
import static org.heigit.ohsome.osm.OSMType.*;

public class UpdateStoreRocksDb implements UpdateStore {

    static {
        RocksDB.loadLibrary();
    }

    public static UpdateStore open(Path path, long cacheSizeInBytes) throws RocksDBException {
        return open(path, cacheSizeInBytes, false);
    }

    public static UpdateStore open(Path path, long cacheSizeInBytes, boolean createIfMissing) throws RocksDBException {
        var cache = new LRUCache(cacheSizeInBytes);

        var options = defaultOptions(cache).setCreateIfMissing(createIfMissing);
        var entities = new EnumMap<OSMType, RocksDB>(OSMType.class);
        entities.put(NODE, RocksDB.open(options, path.resolve("nodes").toString()));
        entities.put(WAY, RocksDB.open(options, path.resolve("ways").toString()));
        entities.put(RELATION, RocksDB.open(options, path.resolve("relations").toString()));

        var optionsWithMerge = defaultOptions(true)
                .setMergeOperator(new StringAppendOperator((char) 0));
        var backRefs = new EnumMap<BackRefs, RocksDB>(BackRefs.class);
        backRefs.put(BackRefs.NODE_WAY, RocksDB.open(optionsWithMerge, path.resolve("node_ways").toString()));

        return new UpdateStoreRocksDb(cache, entities, backRefs);
    }


    private final Cache cache;
    private final Map<OSMType, RocksDB> entities;
    private final Map<BackRefs, RocksDB> backRefs;


    private UpdateStoreRocksDb(Cache cache, Map<OSMType, RocksDB> entities, Map<BackRefs, RocksDB> backRefs) {
        this.cache = cache;
        this.entities = entities;
        this.backRefs = backRefs;
    }


    @Override
    public Map<Long, OSMNode> nodes(Set<Long> ids) {
        try {
            return query(entities.get(NODE), ids, ReplicationEntity::deserializeNode);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void nodes(Map<Long, OSMNode> updates) {
        try {
            updateEntity(entities.get(NODE), updates, ReplicationEntity::serialize);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<Long, OSMWay> ways(Set<Long> ids) {
        try {
            return query(entities.get(WAY), ids, ReplicationEntity::deserializeWay);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void ways(Map<Long, OSMWay> updates) {
        try {
            updateEntity(entities.get(WAY), updates, ReplicationEntity::serialize);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<Long, OSMRelation> relations(Set<Long> ids) {
        try {
            return query(entities.get(RELATION), ids, ReplicationEntity::deserializeRelation);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void relations(Map<Long, OSMRelation> updates) {
        try {
            updateEntity(entities.get(RELATION), updates, ReplicationEntity::serialize);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public Map<Long, Set<Long>> backRefs(BackRefs type, Set<Long> ids) {
        try {
            return query(backRefs.get(type), ids, ReplicationEntity::deserializeSet);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void backRefs(BackRefs type, Map<Long, Set<Long>> updates) {
        try {
            updateBackRefs(backRefs.get(type), updates);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        for (var db : entities.values()) {
            db.close();
        }
        for (var db : backRefs.values()) {
            db.close();
        }

        cache.close();
    }


    private <T> Map<Long, T> query(RocksDB db, Set<Long> ids, BiFunction<Long, byte[], T> fnt) throws RocksDBException {
        var result = Maps.<Long, T>newHashMapWithExpectedSize(ids.size());
        if (ids.isEmpty()) {
            return result;
        }
        var dbIds = ids.stream().map(RocksUtil::key).toList();
        var values = db.multiGetAsList(dbIds);
        for (var i = 0; i < values.size(); i++) {
            var value = values.get(i);
            if (value == null) {
                continue;
            }
            var id = ByteBuffer.wrap(dbIds.get(i)).getLong();
            var t = fnt.apply(id, value);
            result.put(id, t);
        }
        return result;
    }

    private static <T extends OSMEntity> void updateEntity(RocksDB db, Map<Long, T> updates, BiConsumer<T, Output> serialize) throws RocksDBException {
        var output = new Output(4 << 10);
        try (var batch = new WriteBatch();
             var writeOpts = new WriteOptions()) {
            for (var entry : updates.entrySet()) {
                var key = RocksUtil.key(entry.getKey());
                if (!entry.getValue().visible()) {
                    batch.delete(key);
                } else {
                    output.reset();
                    serialize.accept(entry.getValue(), output);
                    batch.put(key, output.array());
                }
            }
            db.write(writeOpts, batch);
        }
    }

    private static void updateBackRefs(RocksDB db, Map<Long, Set<Long>> updates) throws RocksDBException {
        var output = new Output(4 << 10);
        try (var batch = new WriteBatch();
             var writeOpts = new WriteOptions()) {
            for (var entry : updates.entrySet()) {
                var key = RocksUtil.key(entry.getKey());
                output.reset();
                ReplicationEntity.serialize(entry.getValue(), output);
                batch.put(key, output.array());
            }
            db.write(writeOpts, batch);
        }
    }

}
