package org.heigit.ohsome.contributions.minor;

import org.heigit.ohsome.util.io.Input;
import org.heigit.ohsome.util.io.Output;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyMap;

public class MinorNode {

    private MinorNode() {
        // utility class
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static List<OSMNode> deserialize(long id, byte[] bytes) {
        var input = Input.fromBuffer(ByteBuffer.wrap(bytes));
        var size = input.readU32();
        var cs = 0L;
        var ts = 0L;
        var lon = 0L;
        var lat = 0L;
        var osh = new ArrayList<OSMNode>(size);
        var visible = true;
        for (var i = 0; i < size; i++) {
            cs += input.readS64();
            ts += input.readS64();
            var userId = input.readU32();
            var userName = input.readUTF8();
            var deltaLon = input.readS64();
            var deltaLat = input.readS64();
            lon += deltaLon;
            lat += deltaLat;
            visible = deltaLon != 0 || deltaLat != 0 | !visible;
            osh.add(new OSMNode(id, i, Instant.ofEpochSecond(ts), cs, userId, userName, visible, emptyMap(), lon / 1_0000000.0, lat / 1_0000000.0));
        }

        return osh;
    }

    public static class Builder implements MinorBuilder<OSMNode> {
        private final List<OSMNode> versions = new ArrayList<>();
        private boolean visible = false;
        private double lon = Double.NaN;
        private double lat = Double.NaN;

        public void add(OSMNode node) {
            if (node.visible() || visible) {
                if (!node.visible() || !visible || node.lon() != lon && node.lat() != lat) {
                    versions.add(node);
                    lon = node.lon();
                    lat = node.lat();
                }
                visible = node.visible();
            }
        }
        public void serialize(Output output) {
            serialize(output, versions);
        }

        public static void serialize(Output output, List<OSMNode> versions) {
            var size = versions.size();
            if (size == 0) {
                return;
            }
            var cs = 0L;
            var ts = 0L;
            var lon = 0L;
            var lat = 0L;
            output.writeU32(size);
            for (var version : versions) {
                var changeset = version.changeset();
                output.writeS64(changeset - cs);
                cs = changeset;
                var timestamp = version.timestamp();
                output.writeS64(timestamp.getEpochSecond() - ts);
                ts = timestamp.getEpochSecond();

                var userId = version.userId();
                var userName = version.user();
                output.writeU32(userId);
                output.writeUTF8(userName);
                var l = (long) (version.lon() * 1_0000000L);
                output.writeS64(l - lon);
                lon = l;
                l = (long) (version.lat() * 1_0000000L);
                output.writeS64(l - lat);
                lat = l;
            }
        }
    }

}
