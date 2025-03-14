package org.heigit.ohsome.contributions.contrib;

import org.heigit.ohsome.osm.OSMEntity.OSMRelation;
import org.heigit.ohsome.osm.OSMId;

import java.util.*;
import java.util.function.Function;

public class ContributionsRelation extends ContributionsEntity<OSMRelation> {

    public ContributionsRelation(List<OSMRelation> osh, Function<OSMId, Contributions> members) {
        super(osh, members);
    }

    public ContributionsRelation(List<OSMRelation> osh, Map<OSMId, Contributions> oshMembers) {
        super(osh, oshMembers::get);
    }
}
