package org.heigit.ohsome.osm.pbf.group;

import org.heigit.ohsome.osm.OSMEntity.OSMRelation;
import org.heigit.ohsome.osm.OSMType;
import org.heigit.ohsome.osm.pbf.Block;
import org.heigit.ohsome.util.io.Input;

import java.util.ArrayList;
import java.util.List;

public class GroupRelation extends GroupPrimitive<OSMRelation> {
//    public static final int AVG_RELATION_MEMBERS = 200;

    private final List<String> roles = new ArrayList<>();
    private final List<Long> memIds = new ArrayList<>();
    private final List<OSMType> types = new ArrayList<>();


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
        var mTypes = new OSMType[types.size()];
        var mIds = new long[memIds.size()];
        var mRoles = new String[roles.size()];
        for (var i = 0; i < mTypes.length; i++) {
            mTypes[i] = types.get(i);
            mIds[i] = memIds.get(i);
            mRoles[i] = roles.get(i);
        }

        return new OSMRelation(id, version, timestamp, changeset, userId, user, visible, tags(), mTypes, mIds, mRoles, 0, 0);
    }

}
