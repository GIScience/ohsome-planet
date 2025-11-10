package org.heigit.ohsome.osm.changesets;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.zip.GZIPInputStream;

import static java.util.Collections.emptyList;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OSMChangesets {

    private static final XmlMapper xmlMapper;

    static {
        xmlMapper = new XmlMapper();
    }

    public static List<OSMChangeset> readCompressed(InputStream input) throws IOException {
        try (var inputStream = new GZIPInputStream(input)) {
            return readChangesets(inputStream);
        }
    }

    public static List<OSMChangeset> readChangesets(byte[] input) throws IOException {
        return readChangesets(new ByteArrayInputStream(input));
    }

    public static List<OSMChangeset> readChangesets(InputStream input) throws IOException {
        return xmlMapper.readValue(input, OSMChangesets.class).list();
    }

    @JacksonXmlProperty(localName = "changeset")
    @JacksonXmlElementWrapper(useWrapping = false)
    private final List<OSMChangeset> list = emptyList();

    private List<OSMChangeset> list() {
        return list;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public static class OSMChangeset {

        private long id;

        @JsonProperty("created_at")
        private String createdAt;

        @JsonProperty("closed_at")
        private String closedAt;

        private boolean open;

        private String user;

        private int uid;

        @JsonProperty("min_lon")
        public Double minLon;
        @JsonProperty("min_lat")
        public Double minLat;
        @JsonProperty("max_lon")
        public Double maxLon;
        @JsonProperty("max_lat")
        public Double maxLat;

        public String getBBOXasWKT() {
            if (Objects.isNull(minLat) || Double.isNaN(minLat) || Double.isNaN(minLon) || Double.isNaN(maxLat) || Double.isNaN(maxLon)) {
                return null;
            }
            // todo: what happens at antimeridian? Column is currently Polygon-Only
            return String.format("SRID=4326;POLYGON((%f %f, %f %f, %f %f, %f %f, %f %f))",
                    minLon, minLat, maxLon, minLat, maxLon, maxLat, minLon, maxLat, minLon, minLat);
        }

        @JacksonXmlProperty(localName = "tag")
        @JacksonXmlElementWrapper(useWrapping = false)
        private List<Tag> tags = emptyList();

        public static OSMChangeset of(long id, String createdAt, String closedAt, boolean open, String user, int uid, List<Tag> tags) {
            var changeset = new OSMChangeset();
            changeset.id = id;
            changeset.createdAt = createdAt;
            changeset.closedAt = closedAt;
            changeset.open = open;
            changeset.user = user;
            changeset.uid = uid;
            changeset.tags = tags;
            return changeset;
        }

        public long id() {
            return id;
        }

        public String createdAt() {
            return createdAt;
        }

        public String closedAt() {
            return closedAt;
        }

        public boolean isOpen() {
            return open;
        }

        public boolean isClosed() {
            return !open;
        }

        public String user() {
            return user;
        }

        public int userId() {
            return uid;
        }

        public Map<String, String> tags() {
            var map = new LinkedHashMap<String, String>();
            tags.forEach(tag -> map.put(tag.key(), tag.value()));
            return map;
        }

        public Instant getCreatedAt() {
            return parseDate(createdAt);
        }

        public Instant getClosedAt() {
            return parseDate(closedAt);
        }

        private static Instant parseDate(String s) {
            if (s == null) {
                return null;
            }
            return OffsetDateTime.parse(s).toInstant();
        }

        public static class Tag {

            public static Tag of(String k, String v) {
                var tag = new Tag();
                tag.k = k;
                tag.v = v;
                return tag;
            }

            @JsonProperty("k")
            private String k;
            @JsonProperty("v")
            private String v;

            public String key() {
                return k;
            }

            public String value() {
                return v;
            }
        }
    }
}
