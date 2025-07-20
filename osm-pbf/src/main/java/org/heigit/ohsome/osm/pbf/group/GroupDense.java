package org.heigit.ohsome.osm.pbf.group;

import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.pbf.Block;
import org.heigit.ohsome.util.io.Input;
import org.heigit.ohsome.osm.pbf.ProtoZero;

import java.time.Instant;
import java.util.*;

public class GroupDense extends Group<OSMNode> {
    private static final int DEFAULT_ENTITY_SIZE = 8000;

    private final List<Long> ids = new ArrayList<>(DEFAULT_ENTITY_SIZE);
    private final List<Double> lons = new ArrayList<>(DEFAULT_ENTITY_SIZE);
    private final List<Double> lats = new ArrayList<>(DEFAULT_ENTITY_SIZE);
    private final List<Map<String, String>> tags = new ArrayList<>(DEFAULT_ENTITY_SIZE);
    private final Map<String, String> kvs = new LinkedHashMap<>();

    private final List<Integer> versions = new ArrayList<>(DEFAULT_ENTITY_SIZE);
    private final List<Long> timestamps = new ArrayList<>(DEFAULT_ENTITY_SIZE);
    private final List<Long> changesets = new ArrayList<>(DEFAULT_ENTITY_SIZE);
    private final List<Integer> userIds = new ArrayList<>(DEFAULT_ENTITY_SIZE);
    private final List<String> users = new ArrayList<>(DEFAULT_ENTITY_SIZE);
    private final List<Boolean> visibilities = new ArrayList<>(DEFAULT_ENTITY_SIZE);

    // delta coded properties
    private long id;
    private long lon;
    private long lat;
    private long timestamp;
    private long changeset;
    private int userId;
    private int user;

    public GroupDense(Block block) {
        super(block);
    }


    @Override
    public boolean decode(Input input, int tag) {
        switch (tag) {
            case 8 -> ids.add(input.readS64());
            case 10 -> {
                var len = input.readU32();
                var limit = input.pos() + len;
                while (input.pos() < limit) {
                    ids.add((id += input.readS64()));
                }
            }
            case 42 -> ProtoZero.decode(input.readBuffer(), this::parseInfo);
            case 64 -> lats.add(block.parseLat(input.readS64()));
            case 66 -> {
                var len = input.readU32();
                var limit = input.pos() + len;
                while (input.pos() < limit) {
                    lats.add(block.parseLat(lat += input.readS64()));
                }
            }
            case 72 -> lons.add(block.parseLon(input.readS64()));
            case 74 -> {
                var len = input.readU32();
                var limit = input.pos() + len;
                while (input.pos() < limit) {
                    lons.add(block.parseLon(lon += input.readS64()));
                }
            }
            case 80 -> throw new UnsupportedOperationException("primitiveGroup dense single keyvalue");
            case 82 -> {
                var len = input.readU32();
                var limit = input.pos() + len;
                while (input.pos() < limit) {
                    var key = input.readU32();
                    if (key == 0) {
                        tags.add(Map.copyOf(kvs));
                        kvs.clear();
                        continue;
                    }
                    if (input.pos() == limit) {
                        throw new IllegalStateException("no value after key!");
                    }
                    var val = input.readU32();
                    kvs.put(block.string(key), block.string(val));
                }
            }
            default -> {
                return false;
            }
        }
        return true;
    }

    private boolean parseInfo(Input input, int tag) {
        switch (tag) {
            case 8 -> versions.add(input.readU32());
            case 10 -> {
                var len = input.readU32();
                var limit = input.pos() + len;
                while (input.pos() < limit) {
                    versions.add(input.readU32());
                }
            }
            case 16 -> timestamps.add(input.readS64());
            case 18 -> {
                var len = input.readU32();
                var limit = input.pos() + len;
                while (input.pos() < limit) {
                    timestamps.add(block.parseTimestamp(timestamp += input.readS64()));
                }
            }
            case 24 -> changesets.add(input.readS64());
            case 26 -> {
                var len = input.readU32();
                var limit = input.pos() + len;
                while (input.pos() < limit) {
                    changesets.add((changeset += input.readS64()));
                }
            }
            case 32 -> userIds.add(input.readS32());
            case 34 -> {
                var len = input.readU32();
                var limit = input.pos() + len;
                while (input.pos() < limit) {
                    userIds.add((userId += input.readS32()));
                }
            }
            case 40 -> users.add(block.string(input.readS32()));
            case 42 -> {
                var len = input.readU32();
                var limit = input.pos() + len;
                while (input.pos() < limit) {
                    users.add(block.string((user += input.readS32())));
                }
            }
            case 48 -> visibilities.add(input.readBool());
            case 50 -> {
                var len = input.readU32();
                var limit = input.pos() + len;
                while (input.pos() < limit) {
                    visibilities.add(input.readBool());
                }
            }
            default -> {
                return false;
            }
        }
        return true;
    }

    public OSMNode entity(int idx) {
        return new OSMNode(ids.get(idx),
                !versions.isEmpty() ? versions.get(idx) : 0,
                !timestamps.isEmpty() ? Instant.ofEpochMilli(timestamps.get(idx)) : Instant.EPOCH,
                !changesets.isEmpty() ? changesets.get(idx) : 0,
                !userIds.isEmpty() ? userIds.get(idx) : 0,
                !users.isEmpty() ? users.get(idx) : "",
                !visibilities.isEmpty() ? visibilities.get(idx) : true,
                tags.get(idx),
                lons.get(idx),
                lats.get(idx)
        );
    }

    @Override
    public Iterator<OSMNode> iterator() {
        return new EntityIterator();
    }

    private class EntityIterator implements Iterator<OSMNode> {
        private int idx = 0;

        @Override
        public boolean hasNext() {
            return idx < ids.size();
        }

        @Override
        public OSMNode next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return entity(idx++);
        }
    }
}
