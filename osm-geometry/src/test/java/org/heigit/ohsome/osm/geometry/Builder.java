package org.heigit.ohsome.osm.geometry;

import java.util.List;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;

public interface Builder {

  Geometry buildMultiPolygon(List<List<Coordinate>> outer, List<List<Coordinate>> inner);

}
