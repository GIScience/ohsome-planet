package org.heigit.ohsome.replication;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

/**
 * Unit test for simple App.
 */
class ReplicationTest {
    private static final String RESOURCE_PATH = "./src/test/resources";


    @Test
    void testReplication() {
          ReplicationManager.update("minute", Path.of(RESOURCE_PATH), System.getProperty("DB_URL"));
    }
}
