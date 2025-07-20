package org.heigit.ohsome.contributions.contrib;

import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.heigit.ohsome.osm.OSMId;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Optional.ofNullable;
import static org.heigit.ohsome.osm.OSMEntity.OSMNode;

public class ContributionsWay extends ContributionsEntity<OSMWay> {

    public ContributionsWay(List<OSMWay> osh, Function<OSMId, Contributions> members) {
        super(osh, members);
    }

    public ContributionsWay(List<OSMWay> osh, Map<Long, List<OSMNode>> nodes) {
        this(osh, osmId -> ofNullable(nodes.get(osmId.id())).map(id -> (Contributions) new ContributionsNode(id)).orElseGet(() -> new EmptyContributions(osmId)));
    }

}
