package org.heigit.ohsome.contributions.contrib;

import org.heigit.ohsome.osm.OSMId;

public class EmptyContributions extends AbstractContributions {
    protected EmptyContributions(OSMId osmId) {
        super(osmId);
    }

    @Override
    protected Contribution computeNext() {
        return null;
    }
}
