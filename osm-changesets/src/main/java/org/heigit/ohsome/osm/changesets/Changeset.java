package org.heigit.ohsome.osm.changesets;

import java.time.Instant;
import java.util.Map;

public record Changeset(long id,
                        Instant created,
                        Instant closed,
                        String user,
                        int userId,
                        double min_lon,
                        double min_lat,
                        double max_lon,
                        double max_lat,
                        int numChanges,
                        int commentsCount,
                        Map<String, String> tags) {
}
