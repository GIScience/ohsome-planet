package org.heigit.ohsome.osm.geometry;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.heigit.ohsome.osm.geometry.OSMParser.Osm;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * One osm-testdata test case: the metadata from test.json plus the input ways from ways.wkt.
 */
public record OSMTest(
        int testId,
        String description,
        Osm osm,
        AreaEntry defaultResult,
        AreaEntry fixResult,
        Map<Long, LineString> ways
) {

    public boolean isValid() {
        return defaultResult.isValid();
    }

    public boolean isValidWithFix() {
        return isValid() || hasFix();
    }

    public AreaEntry resultWithFix() {
        if (isValid()) return defaultResult;
        if (hasFix()) return fixResult;
        throw new IllegalStateException("Test case " + testId + " is invalid and has no fix!");
    }

    public boolean hasFix() {
        return fixResult != null;
    }

    public record AreaEntry(
            @JsonProperty("from_id")   long fromId,
            @JsonProperty("from_type") String fromType,
                                       String wkt) {
        /** Returns true when this entry carries a real geometry (not the "INVALID" sentinel). */
        public boolean isValid() {
            return !"INVALID".equals(wkt);
        }

    }

    // Jackson helper — mirrors top-level test.json fields
    private static class TestJson {
        @JsonProperty("test_id")    public int testId;
        public String description;
        public Map<String, List<AreaEntry>> areas;
    }

    private static final GeometryFactory FACTORY = new GeometryFactory(new PrecisionModel(1e7));

    public static Stream<OSMTest> loadSilent(Path path) {
        try {
            return Stream.of(load(path));
        } catch (IOException | ParseException e) {
            System.err.println("Failed to load test case from " + path + ": " + e.getMessage());
            return Stream.empty();
        }
    }

    /**
     * Load one test case from a directory that contains {@code test.json} and {@code ways.wkt}.
     *
     * @param path path to the test-case directory (e.g. {@code osm-testdata/grid/data/7/700})
     */
    public static OSMTest load(Path path) throws IOException, ParseException {
        var mapper = new ObjectMapper();
        var raw = mapper.readValue(path.resolve("test.json").toFile(), TestJson.class);
        var osm = OSMParser.parse(path.resolve("data.osm"));

        var ways = new HashMap<Long, LineString>();
        var wktReader = new WKTReader(FACTORY);
        for (var line : Files.readAllLines(path.resolve("ways.wkt"))) {
            if (line.isBlank()) continue;
            var space = line.indexOf(' ');
            var id = Long.parseLong(line, 0, space, 10);
            ways.put(id, (LineString) wktReader.read(line.substring(space + 1)));
        }

        var defaultResult = raw.areas.getOrDefault("location", raw.areas.get("default")).getFirst();
        var fixResult = Optional.ofNullable(raw.areas.get("fix")).map(List::getFirst).orElse(null);
        return new OSMTest(raw.testId, raw.description, osm, defaultResult, fixResult, ways);
    }
}
