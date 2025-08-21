package org.heigit.ohsome.replication.state;

import org.junit.jupiter.api.Test;

import static java.time.Instant.now;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ReplicationStateTest {
    @Test
    void testReplicationPathFormatting() {
        var replicationState = new ReplicationState(now(), 5325622);
        var replicationState2 = new ReplicationState(now(), 1);

        assertEquals("005/325/622", ReplicationState.sequenceNumberAsPath(replicationState.getSequenceNumber()));
        assertEquals("000/000/001", ReplicationState.sequenceNumberAsPath(replicationState2.getSequenceNumber()));
    }
}
