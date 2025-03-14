package org.heigit.ohsome.osm.pbf.group;

import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.heigit.ohsome.util.io.Input;

import java.util.ArrayList;
import java.util.List;

public class GroupWay extends GroupPrimitive<OSMWay> {
    private final List<Long> refs = new ArrayList<>();
    private final List<Long> lats = new ArrayList<>();
    private final List<Long> lons = new ArrayList<>();

    private List<Long> members = new ArrayList<>();

    // delta encoded
    private long ref = 0;
    private long lat = 0;
    private long lon = 0;

    public void clear() {
        super.clear();
        ref = 0;
        refs.clear();
        lat = 0;
        lats.clear();
        lon = 0;
        lons.clear();
    }

    @Override
    public boolean decode(Input input, int tag) {
        if (!super.decode(input, tag)) {
            switch (tag) {
                case 64 -> refs.add(input.readS64());
                case 66 -> {
                    var len = input.readU32();
                    var limit = input.pos() + len;
                    while (input.pos() < limit) {
                        refs.add((ref += input.readS64()));
                    }
                }
                case 72 -> lats.add(input.readS64());
                case 74 -> {
                    var len = input.readU32();
                    var limit = input.pos() + len;
                    while (input.pos() < limit) {
                        lats.add((lat += input.readS64()));
                    }
                }
                case 80 -> lons.add(input.readS64());
                case 82 -> {
                    var len = input.readU32();
                    var limit = input.pos() + len;
                    while (input.pos() < limit) {
                        lons.add((lon += input.readS64()));
                    }
                }
                default -> {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public OSMWay entity() {
        if (!members.equals(refs)) {
            members = List.copyOf(refs);
        }
        return new OSMWay(id, version, timestamp, changeset, userId, user, visible, tags(), members, lons, lats);
    }

}
