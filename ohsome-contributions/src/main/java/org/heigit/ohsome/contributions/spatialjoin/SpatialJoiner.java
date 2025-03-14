package org.heigit.ohsome.contributions.spatialjoin;

import me.tongfei.progressbar.ProgressBar;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.index.hprtree.HPRtree;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.READ;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public interface SpatialJoiner {

    Set<String> join(Geometry geometry);

    static SpatialJoiner noop() {
        return geometry -> Set.of();
    }

    static SpatialJoiner fromCSV(Path path) {
        var prepare = new PreparedGeometryFactory();
        var index = new HPRtree();
        readCSV(path, (id, geom) ->
                index.insert(geom.getEnvelopeInternal(), new SpatialJoinFeature(id, prepare.create(geom))));
        return new SpatialIndexJoiner(index);
    }

    static SpatialJoiner fromCSVGrid(Path path) {
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

    // read csv with ';' separated
    static void readCSV(Path path, BiConsumer<String, Geometry> feature) {
        var wkt = new WKTReader();
        try (var progress = new ProgressBar("read country csv", -1);
             var input = path.toString().endsWith(".gz") ? new GZIPInputStream(Files.newInputStream(path, READ)) : Files.newInputStream(path, READ);
             var lines = new BufferedReader(new InputStreamReader(input, UTF_8))) {
            var line = lines.readLine();
            var headerRow = line.split(";");
            var headers = IntStream.range(0, headerRow.length).boxed().collect(toMap(i -> headerRow[i].toLowerCase(), identity(), Math::min));
            var geomHeader = Stream.of("geometry", "geom", "geo", "wkt")
                    .mapMultiToInt((str, downstream) -> headers.entrySet().stream()
                            .filter(header -> header.getKey().equals(str))
                            .forEach(header -> downstream.accept(header.getValue())))
                    .findFirst().orElseThrow(() -> new RuntimeException("cloud not find a valid geometry header! [geometry, geom, wkt]"));
            var idHeader = Stream.of("id", "iso", "name")
                    .mapMultiToInt((str, downstream) -> headers.entrySet().stream()
                            .filter(header -> header.getKey().startsWith(str))
                            .forEach(header -> downstream.accept(header.getValue())))
                    .findFirst().orElseThrow(() -> new RuntimeException("cloud not find a valid id header! [id*, iso*, name*]"));
            while ((line = lines.readLine()) != null) {
                var row = line.split(";");
                var geometry = wkt.read(row[geomHeader]);
                for (var i = 0; i < geometry.getNumGeometries(); i++) {
                    var part = geometry.getGeometryN(i);
                    feature.accept(row[idHeader], part);
                }
                progress.step();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

}
