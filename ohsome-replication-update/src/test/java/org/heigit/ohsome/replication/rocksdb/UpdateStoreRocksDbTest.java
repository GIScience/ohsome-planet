package org.heigit.ohsome.replication.rocksdb;

import com.google.common.io.MoreFiles;
import org.heigit.ohsome.osm.OSMEntity;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.time.Instant;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class UpdateStoreRocksDbTest {

    @Test
    void updateRocksDb() throws Exception {
        var path = Files.createTempDirectory("test-updatestore");

        try (var store = UpdateStoreRocksDb.open(path, 1 << 20, true)) {
            store.nodes(Map.of(1024L, node(1024, true)));
            store.backRefsNodeWay(Map.of(1024L, Set.of(1L, 2L, 3L)));
        }

        try (var store = UpdateStoreRocksDb.open(path, 1 << 20, false)) {
            var nodes = store.nodes(Set.of(1024L));
            assertEquals(1, nodes.size());
            var node = nodes.get(1024L);
            assertNotNull(node);

            var backRefs = store.backRefsNodeWay(Set.of(1024L));
            assertEquals(1, backRefs.size());
            assertEquals(Set.of(1L, 2L, 3L), backRefs.get(1024L));

            store.nodes(Map.of(1024L, node(1024, false), 1025L, node(1025, true)));
            assertEquals(1, nodes.size());
            nodes =  store.nodes(Set.of(1024L, 1025L));
            node = nodes.get(1025L);
            assertNotNull(node);
        }

        MoreFiles.deleteRecursively(path);
    }

    private static OSMEntity.OSMNode node(long id, boolean visible) {
        return new OSMEntity.OSMNode(id,1, Instant.now(), 1234L, 23, "user", visible, Map.of(), 1.0, 1.0);
    }

}