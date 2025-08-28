package org.heigit.ohsome.replication.processor;

import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.replication.databases.ChangesetDB;
import org.heigit.ohsome.replication.databases.KeyValueDB;

import java.util.List;

import static org.heigit.ohsome.osm.changesets.OSMChangesets.OSMChangeset;

public class ContributionsProcessor {

    private final ChangesetDB changesetDB;
    private final KeyValueDB keyValueDB;

    public ContributionsProcessor(ChangesetDB changesetDB, KeyValueDB keyValueDB) {
        this.changesetDB = changesetDB;
        this.keyValueDB = keyValueDB;
    }


    public void update(List<OSMEntity> entities, int nextSequenceNumber) {

    }

    public void releaseContributions(List<OSMChangeset> changesets) {

    }
}
