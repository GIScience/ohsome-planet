package org.heigit.ohsome.avro;

import org.apache.avro.message.RawMessageDecoder;
import org.apache.avro.message.RawMessageEncoder;
import org.apache.avro.specific.SpecificData;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

class OSMAvroTest {

    @Test
    void encodeDecode() throws IOException {
        var model = new SpecificData();
        var encoder = new RawMessageEncoder<>(model, OSMNode.getClassSchema());
        var decoder = new RawMessageDecoder<>(model, OSMNode.getClassSchema());
        var data = encoder.encode(OSMNode.newBuilder()
                .setTimestamp(Instant.now().getEpochSecond())
                .setTags(Map.of("natrual","tree"))
                // 49.4184793, 8.6756824
                .setLon(86756824).setLat(494184793)
                .setBackRefsWay(List.of(
                        123L, 234L
                ))
                .setBackRefsRelation(List.of())
                .build()).array();

        System.out.printf("data[%d] = %s%n",data.length, Arrays.toString(data));
        var avroNode = decoder.decode(data);
        System.out.println("avroNode = " + avroNode);
    }

}