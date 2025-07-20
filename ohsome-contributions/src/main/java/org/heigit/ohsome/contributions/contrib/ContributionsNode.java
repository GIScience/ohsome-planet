package org.heigit.ohsome.contributions.contrib;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;

import java.util.List;


public class ContributionsNode extends AbstractContributions {

    private final PeekingIterator<OSMNode> majorVersions;

    public ContributionsNode(List<OSMNode> osh) {
        super(osh.getFirst().osmId());
        this.majorVersions = Iterators.peekingIterator(osh.iterator());
    }

    @Override
    protected Contribution computeNext() {
        if (!majorVersions.hasNext()) {
            return null;
        }

        return new Contribution(majorVersions.next());
    }

}
