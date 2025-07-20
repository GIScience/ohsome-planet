package org.heigit.ohsome.contributions.contrib;

import org.heigit.ohsome.osm.OSMId;
import org.heigit.ohsome.osm.OSMType;

import java.util.*;

public abstract class AbstractContributions implements Contributions {


    protected Contribution prev;
    private Contribution next;

    private final OSMId osmId;
    protected AbstractContributions(OSMId osmId) {
        this.osmId = osmId;
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
    public OSMId osmId() {
        return osmId;
    }

    @Override
    public long id() {
        return osmId.id();
    }

    @Override
    public OSMType type() {
        return osmId.type();
    }

    protected abstract Contribution computeNext();

}
