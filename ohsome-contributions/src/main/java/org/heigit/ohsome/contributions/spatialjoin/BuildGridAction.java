package org.heigit.ohsome.contributions.spatialjoin;

import me.tongfei.progressbar.ProgressBar;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.index.hprtree.HPRtree;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;

class BuildGridAction extends RecursiveAction {
    private static final GeometryFactory geometryFactory = new GeometryFactory();

    private final transient ProgressBar progress;
    private final transient HPRtree featureIndex;
    private transient List<SpatialJoinFeature> intersecting;
    private final transient List<SpatialJoinFeature> covering;
    private final transient GridIndex grid;
    private final transient Map<List<SpatialJoinFeature>, Integer> features;
    private final transient int level;
    private final transient int maxLevel;
    private final transient Envelope env;

    public BuildGridAction(ProgressBar progress, HPRtree featureIndex, GridIndex grid, Map<List<SpatialJoinFeature>, Integer> features) {
        this(progress, featureIndex, null ,null, grid, features, 0, 14, new Envelope(-180.0, 180.0, -90.0, 90.0));
    }
    public BuildGridAction(ProgressBar progress, HPRtree featureIndex, List<SpatialJoinFeature> intersecting, List<SpatialJoinFeature> covering, GridIndex grid, Map<List<SpatialJoinFeature>, Integer> features, int level, int maxLevel, Envelope env) {
        this.progress = progress;
        this.featureIndex = featureIndex;
        this.intersecting = intersecting;
        this.covering = covering;
        this.grid = grid;
        this.features = features;
        this.level = level;
        this.maxLevel = maxLevel;
        this.env = env;
    }

    public void compute() {
        var x = env.getMinX() + (env.getMaxX() - env.getMinX()) / 2;
        var y = env.getMinY() + (env.getMaxY() - env.getMinY()) / 2;
        if (level < 4) {
            ForkJoinTask.invokeAll(List.of(
                    new BuildGridAction(progress, featureIndex, null, null, grid, features, level + 1, maxLevel, new Envelope(env.getMinX(), x, env.getMinY(), y)),
                    new BuildGridAction(progress, featureIndex, null, null, grid, features, level + 1, maxLevel, new Envelope(x, env.getMaxX(), env.getMinY(), y)),
                    new BuildGridAction(progress, featureIndex, null, null, grid, features, level + 1, maxLevel, new Envelope(env.getMinX(), x, y, env.getMaxY())),
                    new BuildGridAction(progress, featureIndex, null, null, grid, features, level + 1, maxLevel, new Envelope(x, env.getMaxX(), y, env.getMaxY()))));
            return;
        }

        var bbox = geometryFactory.toGeometry(env);

        //noinspection unchecked
        intersecting = Optional.ofNullable(intersecting).orElseGet(() -> ((List<SpatialJoinFeature>) featureIndex.query(env)));

        var intersects = new ArrayList<SpatialJoinFeature>(intersecting.size());
        var covers = Optional.ofNullable(covering).map(ArrayList::new).orElseGet(ArrayList::new);

        for (var feature : intersecting) {
            if (bbox.intersects(feature.geometry().getGeometry())) {
                if (feature.geometry().covers(bbox)) {
                    covers.add(feature);
                } else {
                    intersects.add(feature);
                }
            }
        }

        if (intersects.isEmpty() && covers.isEmpty()) return;

        if (intersects.isEmpty()) {
            synchronized (grid) {
                var i = features.computeIfAbsent(covers, xyz -> features.size());
                grid.insert(env, i);
            }
            progress.step();
            return;
        }

        if (level >= maxLevel) {
            return;
        }

        ForkJoinTask.invokeAll(List.of(
                new BuildGridAction(progress, featureIndex, intersects, covers, grid, features, level + 1, maxLevel, new Envelope(env.getMinX(), x, env.getMinY(), y)),
                new BuildGridAction(progress, featureIndex, intersects, covers, grid, features, level + 1, maxLevel, new Envelope(x, env.getMaxX(), env.getMinY(), y)),
                new BuildGridAction(progress, featureIndex, intersects, covers, grid, features, level + 1, maxLevel, new Envelope(env.getMinX(), x, y, env.getMaxY())),
                new BuildGridAction(progress, featureIndex, intersects, covers, grid, features, level + 1, maxLevel, new Envelope(x, env.getMaxX(), y, env.getMaxY()))));
    }
}
