package org.heigit.ohsome.changesets;

import com.zaxxer.hikari.HikariDataSource;
import org.heigit.ohsome.osm.changesets.ChangesetDb;
import org.jspecify.annotations.NonNull;

public class TestSupportingChangesetDB extends ChangesetDB{
    public TestSupportingChangesetDB(String connectionString) {
        super(connectionString);
    }

    @Override
    protected @NonNull ChangesetDb createGetterDb(HikariDataSource dataSource) {
        return new TestSupportingChangesetDb(dataSource);
    }
}
