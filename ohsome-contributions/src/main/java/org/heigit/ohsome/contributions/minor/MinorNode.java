package org.heigit.ohsome.contributions.minor;

import org.heigit.ohsome.util.io.Input;
import org.heigit.ohsome.util.io.Output;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;

import java.io.IOException;
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
        var cs = input.readS64();
        var ts = input.readS64();
        var lon = input.readS64();
        var lat = input.readS64();
        var osh = new ArrayList<OSMNode>(size);
        osh.add(new OSMNode(id, 0, Instant.ofEpochSecond(ts), cs, 0, "", true, emptyMap(), lon / 1_0000000.0, lat / 1_0000000.0));
        for (var i = 1; i < size; i++) {
            cs += input.readS64();
            ts += input.readS64();
            var deltaLon = input.readS64();
            var deltaLat = input.readS64();
            lon += deltaLon;
            lat += deltaLat;
            osh.add(new OSMNode(id, i, Instant.ofEpochSecond(ts), cs, 0, "", deltaLon != 0 || deltaLat != 0, emptyMap(), lon / 1_0000000.0, lat / 1_0000000.0));
        }

        return osh;
    }

    public static class Builder implements MinorBuilder<OSMNode> {
        private final List<Long> changesets = new ArrayList<>();
        private final List<Instant> timestamps = new ArrayList<>();
        private final List<Double> lons = new ArrayList<>();
        private final List<Double> lats = new ArrayList<>();
        private long id = -1;

        public Builder() {
            clear();
        }

        public Builder setId(long id) {
            this.id = id;
            return this;
        }

        public void add(OSMNode node) {
            if (!node.visible()) {
                changesets.add(node.changeset());
                timestamps.add(node.timestamp());
                lons.add(lons.getLast());
                lats.add(lats.getLast());
            } else if (node.lon() != lons.getLast() || node.lat() != lats.getLast()) {
                changesets.add(node.changeset());
                timestamps.add(node.timestamp());
                lons.add(node.lon());
                lats.add(node.lat());
            }
        }

        public void clear() {
            id = -1;
            changesets.clear();
            timestamps.clear();
            lons.clear();
            lats.clear();
            lons.add(Double.NaN);
            lats.add(Double.NaN);
        }

        public List<OSMNode> osh() {
            var osh = new ArrayList<OSMNode>(changesets.size());
            osh.add(new OSMNode(id, 0, timestamps.getFirst(), changesets.getFirst(), 0, "", true, emptyMap(), lons.get(1), lats.get(1)));
            for (var i = 1; i < changesets.size(); ++i) {
                osh.add(new OSMNode(id, i, timestamps.get(i), changesets.get(i), 0, "",
                        !(lons.get(i).equals(lons.get(i + 1)) && lats.get(i).equals(lats.get(i + 1)))
                        , emptyMap(), lons.get(i + 1), lats.get(i + 1)));
            }
            return osh;
        }

        public void serialize(Output output) throws IOException {
            var size = changesets.size();
            var cs = 0L;
            var ts = 0L;
            var lon = 0L;
            var lat = 0L;
            output.writeU32(size);
            for (int i = 0; i < size; i++) {
                output.writeS64(changesets.get(i) - cs);
                cs = changesets.get(i);
                output.writeS64(timestamps.get(i).getEpochSecond() - ts);
                ts = timestamps.get(i).getEpochSecond();
                var l = (long) (lons.get(i + 1) * 1_0000000L);
                output.writeS64(l - lon);
                lon = l;
                l = (long) (lats.get(i + 1) * 1_0000000L);
                output.writeS64(l - lat);
                lat = l;
            }
        }
    }

}
