package org.heigit.ohsome.replication;

import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.util.io.Output;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationEntityTest {

    @Test
    void ways() {
        var expected = new OSMEntity.OSMWay(1234L, 2, Instant.parse("2025-10-01T01:00:00Z"), -1, -1, "", true, Map.of("office","HeiGIT"), List.of(1234L, 12345L),
                12, 20, null, null);
        var output = new Output(1024);
        ReplicationEntity.serialize(expected, output);
        var data = output.array();
        var actual = ReplicationEntity.deserializeWay(1234L, data);
        assertEquals(expected, actual);
    }

}