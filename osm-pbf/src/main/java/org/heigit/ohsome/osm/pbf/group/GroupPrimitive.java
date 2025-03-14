package org.heigit.ohsome.osm.pbf.group;

import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.util.io.Input;
import org.heigit.ohsome.osm.pbf.ProtoZero;

import java.time.Instant;
import java.util.*;

public abstract class GroupPrimitive<T extends OSMEntity> extends Group<T> {

    protected long id;
    protected int version;
    protected Instant timestamp;
    protected long changeset;
    protected int userId;
    protected String user;
    protected boolean visible;

    protected final List<String> keys = new ArrayList<>();
    protected final List<String> vals = new ArrayList<>();
    protected final Map<String, String> kvs = new LinkedHashMap<>();

    protected Map<String, String> tags = Map.of();

    public abstract T entity();

    public void clear() {
        id = 0;
        version = 0;
        timestamp = Instant.EPOCH;
        changeset = 0;
        userId = 0;
        user = "";
        visible = true;
        keys.clear();
        vals.clear();
    }

    public Map<String, String> tags() {
        kvs.clear();
        for (var i = 0; i< keys.size(); i++) {
            kvs.put(keys.get(i), vals.get(i));
        }
        if (!tags.equals(kvs)) {
            tags = Map.copyOf(kvs);
        }
        return tags;
    }

    public boolean decode(Input input, int tag) {
        switch (tag) {
            case 8 -> id = input.readU64();
            case 16 -> keys.add(block.string(input.readU32()));
            case 18 -> {
                var len = input.readU32();
                var limit = input.pos() + len;
                while (input.pos() < limit) {
                    keys.add(block.string(input.readU32()));
                }
            }
            case 24 -> vals.add(block.string(input.readU32()));
            case 26 -> {
                var len = input.readU32();
                var limit = input.pos() + len;
                while (input.pos() < limit) {
                    vals.add(block.string(input.readU32()));
                }
            }
            case 34 -> ProtoZero.decode(input.readBuffer(), this::parseInfo);
            default -> {
                return false;
            }
        }
        return true;
    }

    private boolean parseInfo(Input input, int tag) {
        switch(tag) {
            case 8 -> version = input.readU32();
            case 16 -> timestamp = Instant.ofEpochMilli(block.parseTimestamp(input.readU64()));
            case 24 -> changeset = input.readU64();
            case 32 -> userId = input.readU32();
            case 40 -> user = block.string(input.readU32());
            case 48 -> visible = input.readBool();
            default -> {
                return false;
            }
        }
        return true;
    }

    @Override
    public Iterator<T> iterator() {
        return List.of(entity()).iterator();
    }
}
