package org.heigit.ohsome.osm.geometry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import com.fasterxml.jackson.annotation.JsonRootName;
import tools.jackson.dataformat.xml.XmlMapper;
import tools.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import tools.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import java.nio.file.Path;
import java.util.List;

import static tools.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

public class OSMParser {

    public static Osm parse(Path data) {
        return MAPPER.readValue(data, Osm.class);
    }

    public static final XmlMapper MAPPER = XmlMapper.builder()
            .disable(FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    @JsonRootName("osm")
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Osm(
            @JacksonXmlProperty(isAttribute = true) String version,
            @JacksonXmlProperty(isAttribute = true) String generator,
            @JacksonXmlProperty(isAttribute = true) Boolean upload,

            @JacksonXmlElementWrapper(useWrapping = false)
            @JacksonXmlProperty(localName = "node")
            List<Node> nodes,

            @JacksonXmlElementWrapper(useWrapping = false)
            @JacksonXmlProperty(localName = "way")
            List<Way> ways,

            @JacksonXmlElementWrapper(useWrapping = false)
            @JacksonXmlProperty(localName = "relation")
            List<Relation> relations
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Node(
            @JacksonXmlProperty(isAttribute = true) long id,
            @JacksonXmlProperty(isAttribute = true) int version,
            @JacksonXmlProperty(isAttribute = true) String timestamp,
            @JacksonXmlProperty(isAttribute = true) long uid,
            @JacksonXmlProperty(isAttribute = true) String user,
            @JacksonXmlProperty(isAttribute = true) long changeset,
            @JacksonXmlProperty(isAttribute = true) double lon,
            @JacksonXmlProperty(isAttribute = true) double lat,

            @JacksonXmlElementWrapper(useWrapping = false)
            @JacksonXmlProperty(localName = "tag")
            List<Tag> tags
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Way(
            @JacksonXmlProperty(isAttribute = true) long id,
            @JacksonXmlProperty(isAttribute = true) int version,
            @JacksonXmlProperty(isAttribute = true) String timestamp,
            @JacksonXmlProperty(isAttribute = true) long uid,
            @JacksonXmlProperty(isAttribute = true) String user,
            @JacksonXmlProperty(isAttribute = true) long changeset,

            @JacksonXmlElementWrapper(useWrapping = false)
            @JacksonXmlProperty(localName = "nd")
            List<Nd> nds,

            @JacksonXmlElementWrapper(useWrapping = false)
            @JacksonXmlProperty(localName = "tag")
            List<Tag> tags
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Relation(
            @JacksonXmlProperty(isAttribute = true) long id,
            @JacksonXmlProperty(isAttribute = true) int version,
            @JacksonXmlProperty(isAttribute = true) String timestamp,
            @JacksonXmlProperty(isAttribute = true) long uid,
            @JacksonXmlProperty(isAttribute = true) String user,
            @JacksonXmlProperty(isAttribute = true) long changeset,

            @JacksonXmlElementWrapper(useWrapping = false)
            @JacksonXmlProperty(localName = "member")
            List<Member> members,

            @JacksonXmlElementWrapper(useWrapping = false)
            @JacksonXmlProperty(localName = "tag")
            List<Tag> tags
    ) {}

    public record Nd(@JacksonXmlProperty(isAttribute = true) long ref) {}

    public record Tag(
            @JacksonXmlProperty(isAttribute = true) String k,
            @JacksonXmlProperty(isAttribute = true) String v
    ) {}

    public record Member(
            @JacksonXmlProperty(isAttribute = true) String type,
            @JacksonXmlProperty(isAttribute = true) long ref,
            @JacksonXmlProperty(isAttribute = true) String role
    ) {}
}
