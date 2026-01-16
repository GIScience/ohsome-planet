package org.heigit.ohsome.changesets;

import me.tongfei.progressbar.ProgressBarBuilder;
import org.heigit.ohsome.osm.changesets.PBZ2ChangesetReader;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;

import static org.heigit.ohsome.osm.changesets.OSMChangesets.readChangesets;
import static reactor.core.publisher.Mono.fromCallable;
import static reactor.core.scheduler.Schedulers.boundedElastic;
import static reactor.core.scheduler.Schedulers.parallel;

public class Changesets2DB {
    private Changesets2DB() {
        // utility class
    }

    public static int initChangesets(Path changesetsPath, String changesetDbUrl, boolean overwrite, boolean createSchema) throws IOException, SQLException {
        try (var changesetDb = new ChangesetDB(changesetDbUrl)) {
            if (createSchema) {
                changesetDb.createTablesIfNotExists();
            }

            if (overwrite) {
                changesetDb.truncateChangesetTables();
            }

            try (var pb = new ProgressBarBuilder()
                    .setTaskName("Parsed Changesets:")
                    .build()) {
                PBZ2ChangesetReader.read(changesetsPath)
                        .flatMap(bytes -> fromCallable(() -> readChangesets(bytes)).subscribeOn(parallel()))
                        .doOnNext(cs -> pb.stepBy(cs.size()))
                        .flatMap(cs -> fromCallable(() -> changesetDb.changesets2CSV(cs)).subscribeOn(parallel()))
                        .flatMap(
                                changesets -> fromCallable(() -> {
                                    changesetDb.bulkInsertChangesets(changesets);
                                    return changesets;
                                }).subscribeOn(boundedElastic()),
                                changesetDb.getMaxConnections()
                        )
                        .doOnComplete(pb::close)
                        .blockLast();
            }

            return 0;
        }
    }
}
