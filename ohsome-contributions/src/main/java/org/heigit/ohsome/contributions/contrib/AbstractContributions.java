package org.heigit.ohsome.contributions.contrib;

import org.heigit.ohsome.osm.OSMType;

import java.util.*;

public abstract class AbstractContributions implements Contributions {


    protected Contribution prev;
    private Contribution next;

    private final long id;
    private final OSMType type;

    protected AbstractContributions(OSMType type, long id) {
        this.id = id;
        this.type = type;
    }

    @Override
    public boolean hasNext() {
        return next != null || (next = computeNext()) != null;
    }

    @Override
    public Contribution next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        prev = next;
        next = null;
        return prev;
    }

    @Override
    public Contribution peek() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return next;
    }

    @Override
    public Contribution prev() {
        return prev;
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public OSMType type() {
        return type;
    }

    protected abstract Contribution computeNext();

}
