package org.heigit.ohsome.contributions.spatialjoin;

import org.locationtech.jts.geom.Envelope;

public interface SpatialJoinVisitor {

    boolean visit(Envelope env, int item);
}
