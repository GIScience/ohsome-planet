package org.heigit.ohsome.replication.databases;

import org.heigit.ohsome.replication.state.ReplicationState;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KeyValueDBTest {
    private static final String RESOURCE_PATH = "./src/test/resources";
    @Test
    void testUpdateAndGetLocalState() {
        try (var keyValueStore = new KeyValueDB(Path.of(RESOURCE_PATH))){
            var replicationState = new ReplicationState(Instant.now(), 12312);
            keyValueStore.updateLocalState(replicationState);

            var loadedReplication = keyValueStore.getLocalState();

            assertEquals(replicationState, loadedReplication);
        }
    }
}
