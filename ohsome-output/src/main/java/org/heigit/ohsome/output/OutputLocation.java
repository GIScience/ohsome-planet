package org.heigit.ohsome.output;

import java.io.IOException;
import java.nio.file.Path;

public interface OutputLocation extends AutoCloseable {

    void move(Path src, Path dest) throws IOException;

    Path resolve(String other);

    default Path resolve(Path other) {
        return resolve(other.toString());
    }
}
