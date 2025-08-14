package org.heigit.ohsome.osm.changesets;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class PBZ2ReaderTest {
     @Test
    void test() throws Exception {
        try(var channel = Files.newByteChannel(Path.of("/data/changesets-250804.osm.bz2"))){
            var reader = new PBZ2Reader(channel);
            while(reader.hasNext()) {
                reader.next();
            }
        }
     }
}