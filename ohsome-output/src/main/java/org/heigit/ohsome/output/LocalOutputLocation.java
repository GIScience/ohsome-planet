package org.heigit.ohsome.output;

import java.io.IOException;
import java.io.InputStream;
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
    public void delete(Path dest) throws IOException {
        Files.delete(dest);
    }

    @Override
    public void write(Path dest, byte[] data) throws IOException {
        Files.write(dest, data);
    }

    @Override
    public InputStream read(Path dest) throws IOException {
        return Files.newInputStream(path);
    }

    @Override
    public Path resolve(String other) {
        return path.resolve(other);
    }

    @Override
    public String location(Path path) {
        return path.toAbsolutePath().toString();
    }

    @Override
    public boolean exists() throws IOException {
        if (Files.notExists(path)) {
            try (var files = Files.list(path)) {
                return files.iterator().hasNext();
            }
        }
        return false;
    }

    @Override
    public void close() {

    }
}
