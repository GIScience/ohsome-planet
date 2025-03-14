package org.heigit.ohsome.osm;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public sealed interface OSMEntity {

  long id();

  OSMType type();

  default OSMId osmId() {
    return new OSMId(type(), id());
  }

  int version();

  Instant timestamp();

  long changeset();

  int userId();

  String user();

  boolean visible();

  Map<String, String> tags();

  List<OSMMember> members();

  record OSMNode(long id, int version, Instant timestamp, long changeset, int userId, String user,
                 boolean visible,
                 Map<String, String> tags, double lon, double lat) implements OSMEntity {

    @Override
    public OSMType type() {
      return OSMType.NODE;
    }

    @Override
    public List<OSMMember> members() {
      return Collections.emptyList();
    }
  }

  record OSMWay(long id, int version, Instant timestamp, long changeset, int userId, String user,
                boolean visible,
                Map<String, String> tags, List<Long> refs, List<Long> lons,
                List<Long> lats) implements OSMEntity {

    public OSMWay(long id, int version, Instant timestamp, long changeset, int userId, String user,
        boolean visible,
        Map<String, String> tags, List<Long> refs) {
      this(id, version, timestamp, changeset, userId, user, visible, tags, refs, null, null);
    }

    @Override
    public OSMType type() {
      return OSMType.WAY;
    }

    @Override
    public List<OSMMember> members() {
      return refs.stream().map(ref -> new OSMMember(OSMType.NODE, ref, "")).toList();
    }
  }

  record OSMRelation(long id, int version, Instant timestamp, long changeset, int userId,
                     String user, boolean visible,
                     Map<String, String> tags, List<OSMMember> members) implements OSMEntity {

    @Override
    public OSMType type() {
      return OSMType.RELATION;
    }
  }

}
