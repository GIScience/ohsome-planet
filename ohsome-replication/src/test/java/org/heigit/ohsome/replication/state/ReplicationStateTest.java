package org.heigit.ohsome.replication.state;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;

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

    @Test
    void testLocalState() throws IOException {
        var str = """
                #Wed Oct 15 09:49:39 UTC 2025
                sequenceNumber=6815030
                timestamp=2025-10-15T09\\:49\\:15Z
                """;
        var state = ReplicationState.read(str.getBytes());
        assertEquals(6815030, state.getSequenceNumber());
        assertEquals(Instant.parse("2025-10-15T09:49:15Z"), state.getTimestamp());
    }
}
