package org.heigit.ohsome.output;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

public interface OutputLocation extends AutoCloseable {

    void move(Path src, Path dest) throws Exception;

    void delete(Path dest) throws Exception;

    void write(Path dest, byte[] data) throws Exception;

    InputStream read(Path dest) throws Exception;

    Path resolve(String other);

    String location(Path path);

    default Path resolve(Path other) {
        return resolve(other.toString());
    }

    boolean exists() throws IOException;
}
