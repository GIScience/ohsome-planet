package org.heigit.ohsome.changesets;

import com.zaxxer.hikari.HikariDataSource;
import org.heigit.ohsome.osm.changesets.ChangesetDb;


class TestSupportingChangesetDB extends ChangesetDB {

    TestSupportingChangesetDB(String connectionString) {
        super(connectionString);
    }

    @Override
    protected ChangesetDb createGetterDb(HikariDataSource dataSource) {
        return new TestSupportingChangeset_Db(dataSource);
    }

}
