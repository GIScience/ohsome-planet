package org.heigit.ohsome.contributions;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import me.tongfei.progressbar.ProgressBar;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.pbf.Block;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

class OSMIterator implements Iterator<OSMEntity> {
    private final Iterator<Block> blocks;
    private final ProgressBar progress;
    private PeekingIterator<OSMEntity> entities = Iterators.peekingIterator(List.<OSMEntity>of().iterator());
    private OSMEntity next;

    OSMIterator(Iterator<Block> blocks, ProgressBar progress) {
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
