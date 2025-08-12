package org.heigit.ohsome.replication.parser;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.*;

import static org.heigit.ohsome.replication.parser.ChangesetParser.Changeset;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ChangesetParser implements Iterator<Changeset>, AutoCloseable {

    private static final XmlMapper xmlMapper;
    private final XMLStreamReader reader;
    private Changeset next;

    static {
        xmlMapper = new XmlMapper();
    }

    public ChangesetParser(InputStream input) throws XMLStreamException {
        XMLInputFactory factory = XMLInputFactory.newFactory();
        this.reader = factory.createXMLStreamReader(input);
        findNextChangeset();
    }

    private void findNextChangeset() {
        try {
            next = null;
            while (reader.hasNext()) {
                int ev = reader.next();
                if (ev == XMLStreamConstants.START_ELEMENT && "changeset".equals(reader.getLocalName())) {
                    next = xmlMapper.readValue(reader, Changeset.class);
                    break;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error parsing changeset", e);
        }
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public Changeset next() {
        if (next == null) throw new NoSuchElementException();
        Changeset current = next;
        findNextChangeset();
        return current;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Changeset {
        public long id;

        @JacksonXmlProperty(localName = "user")
        public String userName;

        public Long uid;

        @JacksonXmlProperty(localName = "created_at")
        public String createdAtString;

        @JacksonXmlProperty(localName = "closed_at")
        public String closedAtString;

        public Boolean open;

        @JacksonXmlProperty(localName = "min_lon")
        public Double minLon;

        @JacksonXmlProperty(localName = "min_lat")
        public Double minLat;

        @JacksonXmlProperty(localName = "max_lon")
        public Double maxLon;

        @JacksonXmlProperty(localName = "max_lat")
        public Double maxLat;

        @JsonAlias({"changes_count"})
        @JacksonXmlProperty(localName = "num_changes")
        public Integer numChanges;

        @JacksonXmlElementWrapper(useWrapping = false)
        @JacksonXmlProperty(localName = "tag")
        public List<Tag> tags = new ArrayList<>();

        public Map<String, String> tagsAsMap() {
            Map<String, String> m = new HashMap<>();
            if (tags != null) for (Tag t : tags) m.put(t.k, t.v);
            return m;
        }

        public Instant getCreatedAt() {
            return parseDate(createdAtString);
        }

        public Instant getClosedAt() {
            return parseDate(closedAtString);
        }

        private static Instant parseDate(String s) {
            if (s == null) {
                return null;
            }
            return OffsetDateTime.parse(s).toInstant();
        }

        @Override
        public String toString() {
            return "Changeset{id=" + id + ", userName='" + userName + "', uid=" + uid + ", createdAt=" + createdAtString + " , open=" + open + " , numChanges=" + numChanges + " , tags=" + tags + "}";
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Tag {
        public String k;
        public String v;
        @Override
        public String toString() {
            return "Tag{k='" + k + "', v='" + v + "'}";
        }
    }
}

