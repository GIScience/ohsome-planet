package org.heigit.ohsome.osm.changesets;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface Changesets  {
    Changesets NOOP = new Changesets() {
        @Override
        public <T> Map<Long, T> changesets(Set<Long> ids, Factory<T> factory) {
            return new HashMap<>();
        }
    };

    interface Factory<T> {
        T apply(long id, Instant created, Instant closed, Map<String, String> tags, List<String> hashtags, String editor, int numChanges);
    }

    static Changesets open(String changesetDb, int poolSize) {
        if (changesetDb.startsWith("jdbc")) {
            var config = new HikariConfig();
            config.setJdbcUrl(changesetDb);
            config.setMaximumPoolSize(poolSize);
            return new ChangesetDb(new HikariDataSource(config));
        }
        return NOOP;
    }

    <T> Map<Long, T> changesets(Set<Long> ids, Factory<T> factory) throws Exception;
}
