package org.heigit.ohsome.replication;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

/**
 * Unit test for simple App.
 */
public class ReplicationTest {
    @Test
    public void testReplication() {
        var replication = new ReplicationManager();
        replication.update("minute", Path.of(""), System.getProperty("DB_URL"));
    }
}
