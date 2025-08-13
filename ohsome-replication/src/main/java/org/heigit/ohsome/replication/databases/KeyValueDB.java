package org.heigit.ohsome.replication.databases;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.heigit.ohsome.replication.state.ReplicationState;
import org.rocksdb.RocksDB;

import java.nio.file.Files;
import java.nio.file.Path;

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


    public void storeUnclosedContribution() {

    }

    @Override
    public void close() throws Exception {
        STATE_DB.close();
        UNCLOSED_DB.close();
    }
}
