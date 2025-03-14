package org.heigit.ohsome.contributions.minor;

import org.heigit.ohsome.util.io.Output;
import org.heigit.ohsome.osm.OSMEntity;

import java.io.IOException;

public interface MinorBuilder<T extends OSMEntity> {

    void add(T entity) throws IOException;

    void serialize(Output output) throws IOException;
}
