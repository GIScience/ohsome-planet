package org.heigit.ohsome.contributions.minor;

import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.util.io.Output;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.time.Instant.ofEpochSecond;
import static org.junit.jupiter.api.Assertions.*;

class MinorTest {

    @Test
    void testMinorNode() throws IOException {
        var builder = MinorNode.newBuilder();
        builder.add(new OSMEntity.OSMNode(123, 1, ofEpochSecond(1), 1, 1 , "test", true, Map.of(), 0.0, 0.0));

        try (var output = new Output(4 << 10)) {
            builder.serialize(output);
            var bytes = output.array();
            var osh = MinorNode.deserialize(123L, bytes);

            assertEquals(1, osh.size());
            assertEquals("test", osh.getFirst().user());
        }
    }

    @Test
    void testMinorWay() throws IOException {
        var builder = MinorWay.newBuilder();
        builder.add(new OSMWay(123, 1, ofEpochSecond(1), 1, 1, "heigit", true, Map.of(), List.of(3L,4L,5L,1L,2L,3L)));
        builder.add(new OSMWay(123, 2, ofEpochSecond(2), 2, 23, "ohsome", true, Map.of(), List.of(3L,4L,5L,1L,2L,3L)));
        builder.add(new OSMWay(123, 3, ofEpochSecond(3), 3, 999, "test", false, Map.of(), List.of()));
        builder.add(new OSMWay(123, 4, ofEpochSecond(4), 4, 123, "123", true, Map.of(), List.of(1L,2L,3L, 4L, 5L, 6L)));

        try (var output = new Output(4 << 10)){
            builder.serialize(output);
            var bytes = output.array();
            var osh = MinorWay.deserialize(123L, bytes);

            assertEquals(3, osh.size());
            assertArrayEquals(new long[]{3L,4L,5L,1L,2L,3L}, osh.getFirst().refs().stream().mapToLong(Long::longValue).toArray());
            assertArrayEquals(new long[]{}, osh.get(1).refs().stream().mapToLong(Long::longValue).toArray());
            assertArrayEquals(new long[]{1L, 2L, 3L, 4L, 5L, 6L}, osh.get(2).refs().stream().mapToLong(Long::longValue).toArray());

            assertEquals(1, osh.getFirst().userId());
            assertEquals(999, osh.get(1).userId());
            assertEquals(123, osh.get(2).userId());

            assertEquals("heigit", osh.getFirst().user());
            assertEquals("test", osh.get(1).user());
            assertEquals("123", osh.get(2).user());

        }


    }

}