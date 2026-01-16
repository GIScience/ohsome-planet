package org.heigit.ohsome.osm.changesets;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Changesets extends AutoCloseable {
    Changesets NOOP = new Changesets() {
        @Override
        public <T> Map<Long, T> changesets(Set<Long> ids, Factory<T> factory) {
            return new HashMap<>();
        }
    };

    interface Factory<T> {
        T apply(long id, Instant created, Instant closed, Map<String, String> tags, List<String> hashtags, String editor);
    }

//    static Changesets open(String changesetDb, int poolSize) {
//        if (changesetDb.startsWith("jdbc")) {
//            var config = new HikariConfig();
//            config.setJdbcUrl(changesetDb);
//            config.setMaximumPoolSize(poolSize);
//            return new ChangesetDb(new HikariDataSource(config));
//        }
//        return NOOP;
//    }

    @Override
    default void close() throws Exception {

    }

    <T> Map<Long, T> changesets(Set<Long> ids, Factory<T> factory) throws Exception;
}
