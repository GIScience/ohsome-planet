package org.heigit.ohsome.replication;

import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.util.io.Output;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReplicationEntityTest {

    @Test
    void node() {
        var id = 1234L;
        var timestamp = Instant.parse("2025-10-01T12:34:56Z");
        var encode = new OSMEntity.OSMNode(id, 3, timestamp, 12345L, 23, "twentyThree", true,
                Map.of("natural", "tree"),
                8.6756824, 49.4184793);
        var output = new Output(4 << 10);
        ReplicationEntity.serialize(encode, output);
        var bytes = Arrays.copyOf(output.array(), output.length);
        var decode = ReplicationEntity.deserializeNode(id, bytes);

        assertEntityInfo(encode, decode);
        assertEquals(encode.lon(), decode.lon());
        assertEquals(encode.lat(), decode.lat());
    }

    private static void assertEntityInfo(OSMEntity encode, OSMEntity decode) {
        assertEquals(encode.id(), decode.id());
        assertEquals(encode.version(), decode.version());
        assertEquals(encode.timestamp(), decode.timestamp());
        assertEquals(encode.tags(), decode.tags());
        assertEquals(encode.minorVersion(), decode.minorVersion());
        assertEquals(encode.edits(), decode.edits());
    }

    @Test
    void way() {
        var id = 1234L;
        var timestamp = Instant.parse("2025-10-01T12:34:56Z");
        var encode = new OSMEntity.OSMWay(id, 3, timestamp, 12345L, 23, "twentyThree", true,
                Map.of("natural", "tree"), List.of(123456L, 123457L), 5, 10, null, null);
        var output = new Output(4 << 10);
        ReplicationEntity.serialize(encode, output);
        var bytes = Arrays.copyOf(output.array(), output.length);
        var decode = ReplicationEntity.deserializeWay(id, bytes);

        assertEntityInfo(encode, decode);
        assertEquals(encode.refs(), decode.refs());


    }

}
