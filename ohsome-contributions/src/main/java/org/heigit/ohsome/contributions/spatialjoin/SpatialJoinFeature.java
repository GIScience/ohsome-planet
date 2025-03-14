package org.heigit.ohsome.contributions.spatialjoin;

import org.locationtech.jts.geom.prep.PreparedGeometry;

public record SpatialJoinFeature(String id, PreparedGeometry geometry) {
}
