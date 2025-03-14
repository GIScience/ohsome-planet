package org.heigit.ohsome.contributions.spatialjoin;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

public class SpatialIndexJoiner implements SpatialJoiner {
    private final SpatialIndex index;

    public SpatialIndexJoiner(SpatialIndex index) {
        this.index = index;
    }

    @Override
    public Set<String> join(Geometry geometry) {
        return queryFeatureIndex(index, geometry);
    }

    protected Set<String> queryFeatureIndex(SpatialIndex index, Geometry geometry) {
        var set = new HashSet<String>();
        index.query(geometry.getEnvelopeInternal(), o -> Stream.of((SpatialJoinFeature)o)
                .filter(f -> !set.contains(f.id()))
                .filter(f -> f.geometry().intersects(geometry))
                .forEach(f -> set.add(f.id())));
        return set;
    }
}
