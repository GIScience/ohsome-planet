package org.heigit.ohsome.contributions.minor;

import com.google.common.collect.Maps;
import org.heigit.ohsome.util.io.Input;
import org.heigit.ohsome.util.io.Output;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;

public class MinorWay {

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder implements MinorBuilder<OSMWay> {
        private final Set<Long> oshRefs = new TreeSet<>();
        private final List<Long> changesets = new ArrayList<>();
        private final List<Instant> timestamps = new ArrayList<>();

        private final List<List<Long>> allRefs = new ArrayList<>();

        private Builder() {
            clear();
        }

        public void clear() {
            oshRefs.clear();
            changesets.clear();
            timestamps.clear();
            allRefs.clear();
            allRefs.add(List.of());
        }

        public void add(OSMWay way) {
            if (!way.visible()) {
               changesets.add(way.changeset());
               timestamps.add(way.timestamp());
               allRefs.add(List.of());
            } else if (!way.refs().equals(allRefs.getLast())){
                changesets.add(way.changeset());
                timestamps.add(way.timestamp());
                allRefs.add(way.refs());
                oshRefs.addAll(way.refs());
            }
        }

        public void serialize(Output output) throws IOException {
            var size = changesets.size();
            output.writeU32(size);
            output.writeU32(oshRefs.size());
            var lastRef = 0L;
            var map = Maps.<Long, Integer>newHashMapWithExpectedSize(oshRefs.size());
            for (var ref : oshRefs) {
                output.writeU64(ref - lastRef);
                lastRef = ref;
                map.put(ref, map.size());
            }

            var lastCs = 0L;
            var lastTs = 0L;
            for (var i = 0; i < size; i++) {
                var cs = changesets.get(i);
                output.writeS64(cs - lastCs);
                lastCs = cs;
                var ts = timestamps.get(i).getEpochSecond();
                output.writeS64(ts - lastTs);
                lastTs = ts;
                var refs = allRefs.get(i+1);
                output.writeU32(refs.size());
                for(var ref : refs) {
                    output.writeU32(map.get(ref));
                }
            }
        }

    }

    public static List<OSMWay> deserialize(Long id, byte[] bytes) {
        var input = Input.fromBuffer(ByteBuffer.wrap(bytes));
        var size = input.readU32();
        var refSize = input.readU32();
        var map = Maps.<Integer, Long>newHashMapWithExpectedSize(refSize);
        var ref = 0L;
        for(var i=0; i< refSize; i++) {
            ref += input.readU64();
            map.put(i, ref);
        }
        var cs = 0L;
        var ts = 0L;
        var osh = new ArrayList<OSMWay>(size);
        for (var i = 0; i < size; i++) {
            cs += input.readS64();
            ts += input.readS64();
            var length = input.readU32();
            var refs = new ArrayList<Long>(length);
            for(var r = 0; r < length; r++) {
                refs.add(map.get(input.readU32()));
            }
            osh.add(new OSMWay(id, i, Instant.ofEpochSecond(ts), cs, 0, "", true, Map.of(),refs));
        }
        return osh;
    }
}
