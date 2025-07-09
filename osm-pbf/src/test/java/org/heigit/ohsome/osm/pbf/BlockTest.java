package org.heigit.ohsome.osm.pbf;

import com.google.protobuf.ByteString;
import crosby.binary.Osmformat;
import org.heigit.ohsome.osm.OSMEntity;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BlockTest {

    @Test
    void testBlock() {
        var block = Osmformat.PrimitiveBlock.newBuilder();
        block.setStringtable(Osmformat.StringTable.newBuilder()
                .addS(ByteString.copyFromUtf8("")) // Never used.
                .addS(ByteString.copyFromUtf8("natural"))
                .addS(ByteString.copyFromUtf8("tree"))
                .addS(ByteString.copyFromUtf8("heigit"))
                .build()
        );
        block.addPrimitivegroup(Osmformat.PrimitiveGroup.newBuilder()
                .addNodes(Osmformat.Node.newBuilder()
                        .setId(2).setLon(12345).setLat(12345)
                        .addKeys(1).addVals(2)
                        .setInfo(Osmformat.Info.newBuilder()
                                .setChangeset(12345)
                                .setTimestamp(12345)
                                .setVersion(1)
                                .setVisible(true)
                                .setUid(23)
                                .setUserSid(3)))
                .addNodes(Osmformat.Node.newBuilder()
                        .setId(5).setLon(12345).setLat(12345)
                        .addKeys(1).addVals(2)
                        .setInfo(Osmformat.Info.newBuilder()
                                .setChangeset(12345)
                                .setTimestamp(12345)
                                .setVersion(1)
                                .setVisible(false)
                                .setUid(23)
                                .setUserSid(3)))
        );
        block.addPrimitivegroup(Osmformat.PrimitiveGroup.newBuilder()
                .addWays(Osmformat.Way.newBuilder()
                        .setId(2)
                        .addAllRefs(List.of(1L,5L))
                        .addKeys(1).addVals(2)
                        .setInfo(Osmformat.Info.newBuilder()
                                .setChangeset(12345)
                                .setTimestamp(12345)
                                .setVersion(1)
                                .setVisible(true)
                                .setUid(23)
                                .setUserSid(3)))
        );

        var data = block.build().toByteArray();
        ProtoZero.decodeMessage(ByteBuffer.wrap(data), Block::new).entities().forEach(System.out::println);

        var entities = ProtoZero.decodeMessage(ByteBuffer.wrap(data), Block::new).entities().iterator();
        OSMEntity entity;
        assertTrue(entities.hasNext());
        entity = entities.next();
        assertInstanceOf(OSMEntity.OSMNode.class, entity);
        assertTrue(entity.visible());
        assertTrue(entities.hasNext());
        entity = entities.next();
        assertFalse(entity.visible());
        assertTrue(entities.hasNext());
        entity = entities.next();
        assertInstanceOf(OSMEntity.OSMWay.class, entity);
        assertFalse(entities.hasNext());
    }

}