package org.heigit.ohsome.contributions.spatialjoin;

import me.tongfei.progressbar.ProgressBar;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.index.hprtree.HPRtree;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

import static com.google.common.collect.Iterables.any;
import static org.heigit.ohsome.contributions.spatialjoin.SpatialJoiner.readCSV;

public class SpatialGridJoiner implements SpatialJoiner {

  public static SpatialJoiner fromCSVGrid(Path path) {
    var prepare = new PreparedGeometryFactory();
    var featureIndex = new HPRtree();
    var features = new ArrayList<SpatialJoinFeature>();

    readCSV(path, (id, geom) -> features.add(new SpatialJoinFeature(id, prepare.create(geom))));
    features.forEach(feature -> featureIndex.insert(feature.geometry().getGeometry().getEnvelopeInternal(), feature));

    var grid = new GridIndex();
    var gridFeatures = new HashMap<List<SpatialJoinFeature>, Integer>();
    try (var progress = new ProgressBar("building spatial joining grid", -1)) {
      var buildGrid = new BuildGridAction(progress, featureIndex, grid, gridFeatures );
      buildGrid.fork().join();
    }
    grid.build();
    var list = gridFeatures.entrySet().stream().sorted(Map.Entry.comparingByValue()).map(Map.Entry::getKey).toList();
    return new SpatialGridJoiner(list, featureIndex, grid);
  }

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
