package org.heigit.ohsome.osm.pbf.group;

import org.heigit.ohsome.osm.OSMEntity.OSMRelation;
import org.heigit.ohsome.osm.OSMMember;
import org.heigit.ohsome.osm.OSMType;
import org.heigit.ohsome.osm.pbf.Block;
import org.heigit.ohsome.util.io.Input;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupRelation extends GroupPrimitive<OSMRelation> {
    public static final int MAX_RELATION_MEMBERS = 32000;
//    public static final int AVG_RELATION_MEMBERS = 200;

    private final List<String> roles = new ArrayList<>();
    private final List<Long> memIds = new ArrayList<>();
    private final List<OSMType> types = new ArrayList<>();

    private final Map<OSMMember, OSMMember> cache = new HashMap<>();

    private final List<OSMMember> mems = new ArrayList<>(MAX_RELATION_MEMBERS);
    private List<OSMMember> members = new ArrayList<>();

    // delta encoded
    private long memId;

    public GroupRelation(Block block) {
        super(block);
    }

    @Override
    public boolean decode(Input input, int tag) {
        if (!super.decode(input, tag)) {
            switch (tag) {
                case 64 -> roles.add(block.string(input.readU32()));
                case 66 -> {
                    var len = input.readU32();
                    var limit = input.pos() + len;
                    while (input.pos() < limit) {
                        roles.add(block.string(input.readU32()));
                    }
                }
                case 72 -> memIds.add(input.readS64());
                case 74 -> {
                    var len = input.readU32();
                    var limit = input.pos() + len;
                    while (input.pos() < limit) {
                        memIds.add((memId += input.readS64()));
                    }
                }
                case 80 -> types.add(OSMType.parseType(input.readU32()));
                case 82 -> {
                    var len = input.readU32();
                    var limit = input.pos() + len;
                    while (input.pos() < limit) {
                        types.add(OSMType.parseType(input.readU32()));
                    }
                }
                default -> {
                    System.err.println("Unhandled tag: " + tag);
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public OSMRelation entity() {
        return new OSMRelation(id, version, timestamp, changeset, userId, user, visible, tags(), members());
    }

    private List<OSMMember> members() {
        for(var i=0; i<memIds.size(); i++) {
            var member = new OSMMember(types.get(i), memIds.get(i), roles.get(i));
            mems.add(cache.computeIfAbsent(member, k -> member));
        }
        if (!members.equals(mems)) {
            members = List.copyOf(mems);
        }
        return members;
    }
}
