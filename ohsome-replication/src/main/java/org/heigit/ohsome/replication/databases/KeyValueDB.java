package org.heigit.ohsome.replication.databases;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.heigit.ohsome.contributions.avro.Contrib;
import org.heigit.ohsome.replication.state.ReplicationState;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.heigit.ohsome.replication.parser.ChangesetParser.Changeset;

public class KeyValueDB implements AutoCloseable {
    ObjectMapper mapper = new ObjectMapper();
    final RocksDB stateDb;
    final RocksDB unclosedDb;

    public KeyValueDB(Path path) {
        mapper.findAndRegisterModules();
        Path statePath = path.resolve("state");
        Path unclosedPath = path.resolve("unclosed");
        try {
            Files.createDirectories(path);
            Files.createDirectories(statePath);
            Files.createDirectories(unclosedPath);
            stateDb = RocksDB.open(statePath.toString());
            unclosedDb = RocksDB.open(unclosedPath.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public ReplicationState getLocalState() {
        // todo: is this really our local state, or should that just be the last dumped parquet file?
        try {
            return mapper.readValue(stateDb.get("state".getBytes()), ReplicationState.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public void updateLocalState(ReplicationState state) {
        try {
            stateDb.put("state".getBytes(), mapper.writeValueAsBytes(state));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void storeUnclosedContribution(Contrib contrib) {
        try {
            unclosedDb.put(
                    // todo: use contrib id instead for second
                    ("" + contrib.getChangeset().getId() + contrib.getOsmId()).getBytes(),
                    mapper.writeValueAsBytes(contrib));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<Contrib> getUnclosedContributionsForChangeset(Changeset changeset) {
        var collector = new ArrayList<Contrib>();
        try (
                var options = new ReadOptions().setPrefixSameAsStart(true);
                var iter = unclosedDb.newIterator(options)
        ) {
            iter.seek(String.valueOf(changeset.id).getBytes());
            while (iter.isValid()) {
                collector.add(mapper.readValue(iter.value(), Contrib.class));
                iter.next();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return collector;
    }

    @Override
    public void close() {
        stateDb.close();
        unclosedDb.close();
    }
}
