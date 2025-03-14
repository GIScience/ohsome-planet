package org.heigit.ohsome.parquet.contrib;

import org.heigit.ohsome.contributions.contrib.ContributionsAvroConverter;
import org.heigit.ohsome.contributions.contrib.ContributionsNode;
import org.heigit.ohsome.contributions.contrib.ContributionsWay;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.contributions.avro.ContribChangeset;
import org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static java.time.Instant.ofEpochSecond;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.*;

class ContributionsAvroConverterTest {
    private static final ContribChangeset.Builder changesetBuilder = ContribChangeset.newBuilder()
            .setCreatedAt(Instant.now()).setClosedAt(Instant.parse("2222-01-01T00:00:00Z")).setTags(Map.of()).setHashtags(List.of());

    @Test
    void node() {
        var contributions = new ContributionsNode(List.of(
                new OSMEntity.OSMNode(1, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), 0.0, 0.0),
                new OSMEntity.OSMNode(1, 2, ofEpochSecond(2), 2, 2, "", true, emptyMap(), 0.0, 0.0)
        ));

        var converter = new ContributionsAvroConverter(contributions, cs -> changesetBuilder.setId(cs).build(), SpatialJoiner.noop());
        assertTrue(converter.hasNext());
        var contrib = converter.next();
        assertEquals(1, contrib.getOsmVersion());

        assertTrue(converter.hasNext());
        contrib = converter.next();
        assertEquals(2, contrib.getOsmVersion());

        assertFalse(converter.hasNext());
    }

    @Test
    void way() {
        var members = Map.of(
                1L, List.of(
                    new OSMEntity.OSMNode(1, 1, ofEpochSecond(1), 1, 1,"", true, emptyMap(),0.0, 0.0)
                ),
                2L, List.of(
                        new OSMEntity.OSMNode(2, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), 1.0, 0.0)
                )
        );
        var contributions = new ContributionsWay(List.of(
               new OSMEntity.OSMWay(1, 1, ofEpochSecond(1), 1, 1, "", true, emptyMap(), List.of(1L, 2L))
        ), osmId -> new ContributionsNode(members.get(osmId.id())));


        var converter = new ContributionsAvroConverter(contributions, cs -> changesetBuilder.setId(cs).build(), SpatialJoiner.noop());
        assertTrue(converter.hasNext());
        var contrib = converter.next();
        assertEquals("CREATION", contrib.getContribType());

        assertFalse(converter.hasNext());


    }

}