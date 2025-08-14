package org.heigit.ohsome.replication.databases;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.heigit.ohsome.contributions.contrib.Contribution;
import org.heigit.ohsome.replication.state.ReplicationState;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.heigit.ohsome.replication.parser.ChangesetParser.Changeset;

public class KeyValueDB implements AutoCloseable {
    final Path PATH;
    ObjectMapper mapper = new ObjectMapper();
    final RocksDB STATE_DB;
    final RocksDB UNCLOSED_DB;

    KeyValueDB(Path path) {
        mapper.findAndRegisterModules();
        PATH = path;
        Path statePath = PATH.resolve("state");
        Path unclosedPath = PATH.resolve("unclosed");
        try {
            Files.createDirectories(PATH);
            Files.createDirectories(statePath);
            Files.createDirectories(unclosedPath);
            STATE_DB = RocksDB.open(statePath.toString());
            UNCLOSED_DB = RocksDB.open(unclosedPath.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public ReplicationState getLocalState() {
        // todo: is this really our local state, or should that just be the last dumped parquet file?
        try {
            return mapper.readValue(STATE_DB.get("state".getBytes()), ReplicationState.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public void updateLocalState(ReplicationState state) {
        try {
            STATE_DB.put("state".getBytes(), mapper.writeValueAsBytes(state));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void storeUnclosedContribution(Contribution contrib) {
        try {
            UNCLOSED_DB.put(
                    ("" + contrib.changeset() + contrib.entity().id()).getBytes(),
                    mapper.writeValueAsBytes(contrib));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<Contribution> getUnclosedContributionsForChangeset(Changeset changeset) {
        List<Contribution> collector = new ArrayList<>();
        try (
                var options = new ReadOptions().setPrefixSameAsStart(true);
                var iter = UNCLOSED_DB.newIterator(options)
        ) {
            iter.seek(String.valueOf(changeset.id).getBytes());
            while (iter.isValid()) {
                collector.add(mapper.readValue(iter.value(), Contribution.class));
                iter.next();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return collector;
    }

    @Override
    public void close() throws Exception {
        STATE_DB.close();
        UNCLOSED_DB.close();
    }
}
