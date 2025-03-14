package org.heigit.ohsome.contributions.spatialjoin;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.hprtree.HPRtree;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.Iterables.any;

public class SpatialGridJoiner implements SpatialJoiner {

  private final List<List<SpatialJoinFeature>> features;
  private final HPRtree featureIndex;
  private final GridIndex grid;

  public SpatialGridJoiner(List<List<SpatialJoinFeature>> features, HPRtree featureIndex,
      GridIndex grid) {
    this.features = features;
    this.featureIndex = featureIndex;
    this.grid = grid;
  }

  @Override
  public Set<String> join(Geometry geometry) {
    var set = new HashSet<String>();
    var env = geometry.getEnvelopeInternal();
    var visitor = new Visitor(features, set, geometry);
    grid.query(env, visitor);
    if (!visitor.covered) {
      featureIndex.query(geometry.getEnvelopeInternal(), o -> Stream.of((SpatialJoinFeature)o)
          .filter(f -> !set.contains(f.id()))
          .filter(f -> f.geometry().intersects(geometry))
          .forEach(f -> set.add(f.id())));
    }

    return set;
  }

  private static class Visitor implements SpatialJoinVisitor {

    private final Set<String> set;
    private final List<List<SpatialJoinFeature>> features;
    private final Geometry geometry;
    private Envelope env;
    private boolean covered = false;

    private Visitor(List<List<SpatialJoinFeature>> features, Set<String> set, Geometry geometry) {
      this.features = features;
      this.set = set;
      this.geometry = geometry;
      this.env = geometry.getEnvelopeInternal();
    }

    @Override
    public boolean visit(Envelope grid, int item) {
      if (grid.contains(env)) {
        features.get(item).forEach(f -> set.add(f.id()));
        covered = true;
        return false;
      }

      var gridGeom = geometry.getFactory().toGeometry(grid);
      var envGeom = geometry.getFactory().toGeometry(env);
      env = envGeom.intersection(gridGeom).getEnvelopeInternal();

      if (any(features.get(item), f -> !set.contains(f.id())) &&
          gridGeom.intersects(geometry)){
        features.get(item).forEach(f -> set.add(f.id()));
      }
      return true;
    }
  }
}
