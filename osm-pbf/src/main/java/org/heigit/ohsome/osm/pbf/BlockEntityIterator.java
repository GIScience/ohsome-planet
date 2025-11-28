package org.heigit.ohsome.osm.pbf;

import com.google.common.collect.PeekingIterator;
import org.heigit.ohsome.osm.OSMEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.google.common.collect.Iterators.peekingIterator;
import static java.util.Collections.emptyIterator;

public class BlockEntityIterator implements Iterator<List<OSMEntity>> {
    private static final Logger logger = LoggerFactory.getLogger(BlockEntityIterator.class);

    private final List<OSMEntity> osh = new ArrayList<>();

    private final Iterator<Block> blocks;
    private final int limit;


    private int counter = 0;
    private PeekingIterator<OSMEntity> entities = peekingIterator(emptyIterator());
    private List<OSMEntity> next;

    public BlockEntityIterator(Iterator<Block> blocks, int limit) {
        this.blocks = blocks;
        this.limit = limit;
    }

    private void checkNextEntities(int limit) {
        if (counter > limit) {
            entities = peekingIterator(emptyIterator());
            return;
        }

        if (!entities.hasNext() && blocks.hasNext()) {
            entities = peekingIterator(blocks.next().entities().iterator());
            counter++;
        }
    }

    public int counter() {
        return counter;
    }

    @Override
    public boolean hasNext() {
        return next != null || (next = getNext()) != null;
    }

    @Override
    public List<OSMEntity> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        var ret = next;
        next = null;
        return ret;
    }

    private List<OSMEntity> getNext() {
        checkNextEntities(limit);
        if (!entities.hasNext()) {
            return null;
        }
        var type = entities.peek().type();
        var id = entities.peek().id();
        var c = counter;
        osh.clear();
        while (entities.hasNext() && entities.peek().type() == type && entities.peek().id() == id) {
            if (c != counter) {
                logger.trace("cross boundaries! {}/{}", type, id);
                c = counter;
            }
            osh.add(entities.next());
            checkNextEntities(Integer.MAX_VALUE);
        }
        return new ArrayList<>(osh);
    }
}
