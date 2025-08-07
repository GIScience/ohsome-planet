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
        builder.add(new OSMEntity.OSMNode(123, 0, ofEpochSecond(0), 0, 1 , "test", false, Map.of(), 0.0, 0.0));
        builder.add(new OSMEntity.OSMNode(123, 1, ofEpochSecond(1), 1, 1 , "test", true, Map.of(), 0.0, 0.0));
        builder.add(new OSMEntity.OSMNode(123, 2, ofEpochSecond(2), 2, 1 , "test", true, Map.of(), 1.0, 1.0));
        builder.add(new OSMEntity.OSMNode(123, 3, ofEpochSecond(3), 3, 1 , "test", true, Map.of("node","test"), 1.0, 1.0));
        builder.add(new OSMEntity.OSMNode(123, 4, ofEpochSecond(4), 4, 1 , "test", true, Map.of(), 2.0, 2.0));
        builder.add(new OSMEntity.OSMNode(123, 5, ofEpochSecond(5), 5, 1 , "test", false, Map.of(), 2.0, 2.0));
        builder.add(new OSMEntity.OSMNode(123, 6, ofEpochSecond(6), 6, 6 , "6", true, Map.of(), 2.0, 2.0));

        try (var output = new Output(4 << 10)) {
            builder.serialize(output);
            var bytes = output.array();
            var osh = MinorNode.deserialize(123L, bytes);

            assertEquals(5, osh.size());

            assertEquals("test", osh.get(0).user());
            assertEquals(0.0, osh.get(0).lon());
            assertEquals(1.0, osh.get(1).lon());
            assertEquals(2.0, osh.get(2).lon());

            assertEquals(2.0, osh.get(3).lon());
            assertFalse(osh.get(3).visible());

            assertEquals(2.0, osh.get(4).lon());
            assertTrue(osh.get(4).visible());
            assertEquals(6, osh.get(4).userId());
            assertEquals("6", osh.get(4).user());
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