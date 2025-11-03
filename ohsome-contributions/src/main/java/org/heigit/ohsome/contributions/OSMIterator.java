package org.heigit.ohsome.contributions;

import org.heigit.ohsome.contributions.util.Progress;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.pbf.Block;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class OSMIterator implements Iterator<OSMEntity>, AutoCloseable {
    private final Iterator<Block> blocks;
    private final Progress progress;
    private Iterator<OSMEntity> entities;
    private OSMEntity next;

    public OSMIterator(Iterator<Block> blocks, Progress progress) {
        this.blocks = blocks;
        this.entities = blocks.hasNext() ? blocks.next().entities().iterator() :  Collections.emptyIterator();
        this.progress = progress;
    }

    @Override
    public boolean hasNext() {
        return (next != null) || (next = getNextEntity()) != null;
    }

    @Override
    public OSMEntity next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        var ret = next;
        next = null;
        return ret;
    }

    private OSMEntity getNextEntity() {
        if (!entities.hasNext()) {
            if (!blocks.hasNext()) {
                return null;
            }
            progress.step();
            entities = blocks.next().entities().iterator();
        }
        return entities.next();
    }

    @Override
    public void close() {
        progress.step();
    }
}
