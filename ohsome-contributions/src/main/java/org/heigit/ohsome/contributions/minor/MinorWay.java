package org.heigit.ohsome.contributions.minor;

import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.heigit.ohsome.util.io.Input;
import org.heigit.ohsome.util.io.Output;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;

public class MinorWay {

    private MinorWay() {
        // utility class
    }


    public static Builder newBuilder() {
        return new Builder();
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
        var userId = 0;
        var userName = "";
        var osh = new ArrayList<OSMWay>(size);
        for (var i = 0; i < size; i++) {
            cs += input.readS64();
            ts += input.readS64();
            userId = input.readU32();
            userName = input.readUTF8();
            var length = input.readU32();
            var refs = new long[length];
            for(var r = 0; r < length; r++) {
                refs[r] = map.get(input.readU32());
            }
            osh.add(new OSMWay(id, i, Instant.ofEpochSecond(ts), cs, userId, userName, true, Map.of(),refs));
        }
        return osh;
    }

    public static class Builder implements MinorBuilder<OSMWay> {
        private final Set<Long> oshRefs = new TreeSet<>();
        private final List<Long> changesets = new ArrayList<>();
        private final List<Instant> timestamps = new ArrayList<>();
        private final List<Integer> userIds = new ArrayList<>();
        private final List<String> userNames = new ArrayList<>();

        private final List<List<Long>> allRefs = new ArrayList<>();

        private Builder() {
            clear();
        }

        public void clear() {
            oshRefs.clear();
            changesets.clear();
            timestamps.clear();
            userIds.clear();
            userNames.clear();
            allRefs.clear();
            allRefs.add(List.of());
        }

        public void add(OSMWay way) {
            if (!way.visible()) {
               changesets.add(way.changeset());
               timestamps.add(way.timestamp());
               userIds.add(way.userId());
               userNames.add(way.user());
               allRefs.add(List.of());
               return;
            }
            var refList = LongArrayList.wrap(way.refs());
            if (!refList.equals(allRefs.getLast())){
                changesets.add(way.changeset());
                timestamps.add(way.timestamp());
                userIds.add(way.userId());
                userNames.add(way.user());
                allRefs.add(refList);
                oshRefs.addAll(refList);
            }
        }

        public void serialize(Output output) {
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

            var cs = 0L;
            var ts = 0L;
            for (var i = 0; i < size; i++) {
                output.writeS64(changesets.get(i) - cs);
                cs = changesets.get(i);
                output.writeS64(timestamps.get(i).getEpochSecond() - ts);
                ts = timestamps.get(i).getEpochSecond();
                output.writeU32(userIds.get(i));
                output.writeUTF8(userNames.get(i));
                var refs = allRefs.get(i+1);
                output.writeU32(refs.size());
                for(var ref : refs) {
                    output.writeU32(map.get(ref));
                }
            }
        }

    }


}
