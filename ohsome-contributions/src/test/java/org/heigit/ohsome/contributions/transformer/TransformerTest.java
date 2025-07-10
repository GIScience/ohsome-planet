package org.heigit.ohsome.contributions.transformer;

import com.google.protobuf.ByteString;
import crosby.binary.Osmformat;
import org.heigit.ohsome.contributions.OSMIterator;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.pbf.Block;
import org.heigit.ohsome.osm.pbf.ProtoZero;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static junit.framework.Assert.*;

class TransformerTest {

    static final Osmformat.StringTable stringTable = Osmformat.StringTable.newBuilder()
            .addS(ByteString.copyFromUtf8("")) // Never used.
            .addS(ByteString.copyFromUtf8("natural"))
            .addS(ByteString.copyFromUtf8("tree"))
            .addS(ByteString.copyFromUtf8("heigit"))
            .build();

    @Test
    void testOSMIterator() {
        var blocks = List.of(
                ProtoZero.decodeMessage(ByteBuffer.wrap(
                        Osmformat.PrimitiveBlock.newBuilder()
                                .setStringtable(stringTable)
                                .addPrimitivegroup(Osmformat.PrimitiveGroup.newBuilder()
                                        .addWays(Osmformat.Way.newBuilder()
                                                .setId(2)
                                                .addAllRefs(List.of(1L, 5L))
                                                .addKeys(1).addVals(2)
                                                .setInfo(Osmformat.Info.newBuilder()
                                                        .setChangeset(12345)
                                                        .setTimestamp(12345)
                                                        .setVersion(1)
                                                        .setVisible(true)
                                                        .setUid(23)
                                                        .setUserSid(3)))).build().toByteArray()), Block::new),
                ProtoZero.decodeMessage(ByteBuffer.wrap(
                        Osmformat.PrimitiveBlock.newBuilder()
                                .setStringtable(stringTable)
                                .addPrimitivegroup(Osmformat.PrimitiveGroup.newBuilder()
                                        .addWays(Osmformat.Way.newBuilder()
                                                .setId(2)
                                                .addAllRefs(List.of(1L, 5L))
                                                .addKeys(1).addVals(2)
                                                .setInfo(Osmformat.Info.newBuilder()
                                                        .setChangeset(22345)
                                                        .setTimestamp(22345)
                                                        .setVersion(2)
                                                        .setVisible(true)
                                                        .setUid(23)
                                                        .setUserSid(3)))).build().toByteArray()), Block::new),
                ProtoZero.decodeMessage(ByteBuffer.wrap(
                        Osmformat.PrimitiveBlock.newBuilder()
                                .setStringtable(stringTable)
                                .addPrimitivegroup(Osmformat.PrimitiveGroup.newBuilder()
                                        .addWays(Osmformat.Way.newBuilder()
                                                .setId(3)
                                                .addAllRefs(List.of(1L, 5L))
                                                .addKeys(1).addVals(2)
                                                .setInfo(Osmformat.Info.newBuilder()
                                                        .setChangeset(22345)
                                                        .setTimestamp(22345)
                                                        .setVersion(2)
                                                        .setVisible(true)
                                                        .setUid(23)
                                                        .setUserSid(3)))).build().toByteArray()), Block::new),
                ProtoZero.decodeMessage(ByteBuffer.wrap(
                        Osmformat.PrimitiveBlock.newBuilder()
                                .setStringtable(stringTable)
                                .addPrimitivegroup(Osmformat.PrimitiveGroup.newBuilder()
                                        .addWays(Osmformat.Way.newBuilder()
                                                .setId(3)
                                                .addAllRefs(List.of(1L, 5L))
                                                .addKeys(1).addVals(2)
                                                .setInfo(Osmformat.Info.newBuilder()
                                                        .setChangeset(22345)
                                                        .setTimestamp(22345)
                                                        .setVersion(1)
                                                        .setVisible(true)
                                                        .setUid(23)
                                                        .setUserSid(3)))).build().toByteArray()), Block::new)
        );

        var entities = new OSMIterator(blocks.iterator(), step -> {});
        OSMEntity entity;
        assertTrue(entities.hasNext());
        entity = entities.next();
        assertEquals(2, entity.id());

        assertTrue(entities.hasNext());
        entity = entities.next();
        assertEquals(2, entity.id());

        assertTrue(entities.hasNext());
        entity = entities.next();
        assertEquals(3, entity.id());

        assertTrue(entities.hasNext());
        entity = entities.next();
        assertEquals(3, entity.id());

        assertFalse(entities.hasNext());
    }
}
