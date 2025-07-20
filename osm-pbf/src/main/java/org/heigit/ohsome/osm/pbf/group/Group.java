package org.heigit.ohsome.osm.pbf.group;

import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.pbf.Block;
import org.heigit.ohsome.osm.pbf.ProtoZero;

public abstract class Group<T extends OSMEntity> implements ProtoZero.Message, Iterable<T> {
    protected Block block;

    protected Group(Block block) {
        this.block = block;
    }

    @Override
    public void finish() {
        this.block = null;
    }
}
