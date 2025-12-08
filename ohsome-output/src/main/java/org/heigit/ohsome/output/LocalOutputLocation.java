package org.heigit.ohsome.output;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class LocalOutputLocation implements OutputLocation {

    private final Path path;

    public LocalOutputLocation(Path path) {
        this.path = path;
    }

    @Override
    public void move(Path src, Path dest) throws IOException {
        Files.createDirectories(dest.getParent());
        Files.move(src, dest, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public Path resolve(String other) {
        return path.resolve(other);
    }

    @Override
    public void close() {

    }
}
