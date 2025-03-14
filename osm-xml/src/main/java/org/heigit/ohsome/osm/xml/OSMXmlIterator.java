package org.heigit.ohsome.osm.xml;

import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;

import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import javax.management.modelmbean.XMLParseException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.heigit.ohsome.osm.OSMEntity;

public class OSMXmlIterator implements Iterator<OSMEntity>, AutoCloseable {

  private static final XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
  private final OSMXmlEntityReader entityReader = new OSMXmlEntityReader();
  private final XMLStreamReader reader;

  private OSMEntity next;

  public OSMXmlIterator(InputStream input) throws XMLStreamException, XMLParseException {
    this.reader = xmlInputFactory.createXMLStreamReader(input, "UTF8");
    var eventType = reader.nextTag();
    if (eventType != START_ELEMENT) {
      throw new XMLParseException(
          "expecting start of element (" + START_ELEMENT + ") but got " + eventType);
    }
    var localName = reader.getLocalName();
    if (!"osm".equals(localName)) {
      throw new XMLParseException("expecting tag(osm) but got %s".formatted(localName));
    }
  }

  private OSMEntity computeNext() {
    try {
      return entityReader.entity(reader, true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean hasNext() {
    return next != null || (next = computeNext()) != null;
  }

  @Override
  public OSMEntity next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    var prev = next;
    next = null;
    return prev;
  }

  @Override
  public void close() throws Exception {
    reader.close();
  }
}
