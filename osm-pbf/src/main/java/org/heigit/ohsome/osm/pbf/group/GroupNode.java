package org.heigit.ohsome.osm.pbf.group;

import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.pbf.Block;
import org.heigit.ohsome.util.io.Input;

public class GroupNode extends GroupPrimitive<OSMNode> {
    private double lon;
    private double lat;

    public GroupNode(Block block) {
        super(block);
    }


    @Override
    public boolean decode(Input input, int tag) {
        if (tag == 8) {
            id = input.readS64();
            return true;
        }
        if (!super.decode(input, tag)) {
            switch (tag) {
                case 64 -> lat = block.parseLat(input.readS64());
                case 72 -> lon = block.parseLon(input.readS64());
                default -> {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public OSMNode entity() {
        return new OSMNode(id, version, timestamp, changeset, userId, user, visible, tags(), lon, lat);
    }

}
