package org.heigit.ohsome.changesets;

import org.heigit.ohsome.osm.changesets.Changesets;
import org.heigit.ohsome.replication.ReplicationState;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.time.Instant.now;

public interface IChangesetDB extends Changesets, AutoCloseable {

    IChangesetDB NOOP = new IChangesetDB() {
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

        @Override
        public ReplicationState getLocalState() throws SQLException {
            return new ReplicationState(now(), 1);
        }

    };

    static IChangesetDB noop() {return NOOP;}

    @Override
    <T> Map<Long, T> changesets(Set<Long> ids, Factory<T> factory) throws Exception;

    void pendingChangesets(Set<Long> ids) throws SQLException;

    ReplicationState getLocalState() throws SQLException;

}
