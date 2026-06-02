package org.heigit.ohsome.contributions.contrib;

import org.heigit.ohsome.osm.OSMType;

public class EmptyContributions extends AbstractContributions {
    protected EmptyContributions(OSMType type, long id) {
        super(type, id);
    }

    @Override
    protected Contribution computeNext() {
        return null;
    }
}
