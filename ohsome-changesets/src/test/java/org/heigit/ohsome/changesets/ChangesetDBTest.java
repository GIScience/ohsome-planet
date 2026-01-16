package org.heigit.ohsome.changesets;

import org.heigit.ohsome.osm.changesets.OSMChangesets;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
class ChangesetDBTest {

    @Container
    private static final PostgreSQLContainer postgresContainer = new PostgreSQLContainer(
            DockerImageName.parse("postgis/postgis:17-3.5"
            ).asCompatibleSubstituteFor("postgres"))
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass");


    private static String dbUrl;

    @BeforeAll
    static void setUp() {
        postgresContainer.start();
        dbUrl = postgresContainer.getJdbcUrl() + "&user=" + postgresContainer.getUsername() + "&password=" + postgresContainer.getPassword();
    }

    @AfterAll
    static void tearDown() {
        postgresContainer.stop();
    }

    record TestChangeset(long id, Instant created, Instant closed, Map<String, String> tags, List<String> hashtags, String editor){}

    @Test
    void test() throws Exception {
        try(var changesetDb = new ChangesetDB(dbUrl)) {
            changesetDb.createTablesIfNotExists();

            changesetDb.pendingChangesets(Set.of(12345L));

            changesetDb.upsertChangesets(List.of(
                    OSMChangesets.OSMChangeset.of(12345L,
                            "2026-01-05T19:54:13Z",
                            null,
                            true,
                            "ohsome",
                            23,
                            List.of())
            ));
            var changesets = changesetDb.changesets(Set.of(12345L), TestChangeset::new);
            var changeset = (TestChangeset) null;

            changeset = changesets.get(12345L);
            assertNotNull(changeset);

            changesetDb.upsertChangesets(List.of(
                    OSMChangesets.OSMChangeset.of(12345L,
                            "2026-01-05T19:54:13Z",
                            "2026-01-06T19:54:13Z",
                            false,
                            "ohsome",
                            23,
                            List.of())
            ));

            changesetDb.upsertChangesets(List.of(
                    OSMChangesets.OSMChangeset.of(12345L,
                            "2025-01-05T19:54:13Z",
                            null,
                            true,
                            "ohsome",
                            23,
                            List.of())
            ));

            changesets = changesetDb.changesets(Set.of(12345L), TestChangeset::new);
            changeset = changesets.get(12345L);
            assertNotNull(changeset);

            assertEquals(Instant.parse("2026-01-05T19:54:13Z"),  changeset.created());
            assertNotNull(changeset.closed());

        }
    }
  
}