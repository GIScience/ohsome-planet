package org.heigit.ohsome.contributions;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.heigit.ohsome.contributions.util.Progress;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.pbf.Block;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class OSMIterator implements Iterator<OSMEntity> {
    private final Iterator<Block> blocks;
    private final Progress progress;
    private PeekingIterator<OSMEntity> entities = Iterators.peekingIterator(Collections.emptyIterator());
    private OSMEntity next;

    public OSMIterator(Iterator<Block> blocks, Progress progress) {
        this.blocks = blocks;
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
            entities = Iterators.peekingIterator(blocks.next().entities().iterator());
        }
        var next = entities.next();
        if (!entities.hasNext()) {
            progress.step();
        }
        return next;
    }
}
