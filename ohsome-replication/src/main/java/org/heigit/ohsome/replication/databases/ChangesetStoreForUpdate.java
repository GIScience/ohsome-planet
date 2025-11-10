package org.heigit.ohsome.replication.databases;

import org.heigit.ohsome.osm.changesets.Changesets;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public interface ChangesetStoreForUpdate extends Changesets, AutoCloseable {

    ChangesetStoreForUpdate NOOP = new ChangesetStoreForUpdate() {
        @Override
        public void close() throws Exception {

        }

        @Override
        public <T> Map<Long, T> changesets(Set<Long> ids, Factory<T> factory) {
            return new HashMap<>();
        }

        @Override
        public void pendingChangesets(Set<Long> ids) throws SQLException {

        }
    };

    static ChangesetStoreForUpdate noop() {return NOOP;}
    public

    @Override
    <T> Map<Long, T> changesets(Set<Long> ids, Factory<T> factory) throws Exception;

    void pendingChangesets(Set<Long> ids) throws SQLException;
}
