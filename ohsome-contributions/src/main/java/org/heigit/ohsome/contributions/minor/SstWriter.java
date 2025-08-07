package org.heigit.ohsome.contributions.minor;

import org.heigit.ohsome.util.io.Output;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.OSMEntity.OSMNode;
import org.heigit.ohsome.osm.OSMEntity.OSMWay;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class SstWriter implements AutoCloseable {
    private final SstFileWriter writer;
    private final Output output = new Output(4 << 10);
    private final ByteBuffer keyBuffer = ByteBuffer.allocateDirect(Long.BYTES).order(ByteOrder.BIG_ENDIAN);
    private ByteBuffer valBuffer = ByteBuffer.allocateDirect(4 << 10); // 4kb
    private long counter = 0;

    public SstWriter(Path path, SstFileWriter writer) throws IOException, RocksDBException {
        this.writer = writer;
        Files.createDirectories(path.getParent());
        writer.open(path.toString());
    }

    @Override
    public void close() throws Exception {
        try {
            if (counter > 0) {
                writer.finish();
            }
        } finally {
            writer.close();
        }
    }

    public void writeMinorNode(List<OSMNode> osh) throws RocksDBException, IOException {
        var id = osh.getFirst().id();
        var minorNodeBuilder = MinorNode.newBuilder();
        write(id, osh, minorNodeBuilder);
    }

    private <T extends OSMEntity> void write(long id, List<T> osh, MinorBuilder<T> builder) throws RocksDBException, IOException {
        for (T osm : osh) {
            builder.add(osm);
        }

        output.reset();
        builder.serialize(output);
        keyBuffer.clear().putLong(id).flip();
        if (output.length > valBuffer.capacity()) {
            valBuffer = ByteBuffer.allocateDirect(output.length);
        }
        valBuffer.clear().put(output.array, 0, output.length).flip();
        writer.put(keyBuffer, valBuffer);
        counter++;
    }

    public void writeMinorWay(List<OSMWay> osh) throws IOException, RocksDBException {
        var id = osh.getFirst().id();
        var minorWayBuilder = MinorWay.newBuilder();
        write(id, osh, minorWayBuilder);
    }
}
