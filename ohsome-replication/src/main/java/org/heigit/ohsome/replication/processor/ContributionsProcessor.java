package org.heigit.ohsome.replication.processor;

import com.google.common.base.Stopwatch;
import org.heigit.ohsome.contributions.transformer.Transformer;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.replication.databases.ChangesetDB;
import org.heigit.ohsome.replication.update.ContributionUpdater;
import org.heigit.ohsome.replication.update.UpdateStore;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.heigit.ohsome.osm.changesets.OSMChangesets.OSMChangeset;

public class ContributionsProcessor {

    private final UpdateStore store = new UpdateStore();
    private final Path output = Path.of(".");
    private final ChangesetDB changesetDb;

    public ContributionsProcessor(ChangesetDB changesetDb) {
      this.changesetDb = changesetDb;
    }


    public void update(List<OSMEntity> entities, int sequenceNumber) throws IOException {
        var updater = new ContributionUpdater(store);

        var path = output.resolve("ohsome-planet-diff-%09d.parquet".formatted(sequenceNumber));
        var unclosedPath = output.resolve("unclosed").resolve("ohsome-planet-diff-unclosed-%09d.parquet".formatted(sequenceNumber));
        Files.createDirectories(unclosedPath.toAbsolutePath().getParent());
        var stopwatch = Stopwatch.createStarted();
        var count = 0L;
        try (var writer = Transformer.openWriter(path, config -> {});
             var unclosedWriter = Transformer.openWriter(unclosedPath, config -> {})) {
            count = updater.update(entities.iterator())
                    .concatMap(contrib -> Mono.fromCallable(() -> {
                        writer.write(contrib);
                        if (contrib.getChangeset().getClosedAt() == null) {
                            unclosedWriter.write(contrib);
                        }
                        return 1;
                    })).count().blockOptional().orElseThrow();
        }
        System.out.printf("update %s (%d) in %s%n", path, count, stopwatch);
    }

    public void releaseContributions(List<OSMChangeset> changesets) {

    }
}
