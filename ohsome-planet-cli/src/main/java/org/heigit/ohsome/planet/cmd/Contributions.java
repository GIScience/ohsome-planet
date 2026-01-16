package org.heigit.ohsome.planet.cmd;

import com.google.common.io.MoreFiles;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import org.heigit.ohsome.contributions.Contributions2Parquet;
import org.heigit.ohsome.planet.converter.UrlConverter;
import org.heigit.ohsome.planet.utils.CliUtils;
import org.heigit.ohsome.planet.utils.ManifestVersionProvider;
import picocli.CommandLine;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;

@CommandLine.Command(name = "contributions",
        mixinStandardHelpOptions = true,
        versionProvider = ManifestVersionProvider.class,
        usageHelpWidth = 120,
        sortSynopsis = false,
        requiredOptionMarker = '*',
        description = """
                Transform OSM (history/latest) .pbf file into parquet format.
                """
)
public class Contributions implements Callable<Integer> {

    private static final String OHSOME_PLANET_METRICS_PORT = "OHSOME_PLANET_METRICS_PORT";

    @CommandLine.Option(names = {"--pbf"}, required = true,
            paramLabel = "path/to/osm.pbf",
            description = "OSM (history/latest) .pbf file path.")
    private Path pbfPath;

    @CommandLine.Option(names = {"--data"}, required = true,
            paramLabel = "path/to/ohsome-planet-data",
            description = """
                    ohsome-planet working directory for parquet-data (contributions), temp files (temp) and optional replication-store (replication).""")
    private Path data;

    @CommandLine.Option(names = {"--parquet-data"},
            paramLabel = "s3://BUCKET/PATH",
            description = """
                    Write parquet files into separate output directory in your filesystem or S3 cloud storage.
                    For S3 cloud storage set environment variables (S3_ENDPOINT, S3_KEY_ID, S3_SECRET, S3_REGION).
                    """)
    private String parquetData;

    @CommandLine.Option(names = {"--overwrite"},
            description = "Remove all files in data directory if exists."
    )
    private boolean overwrite = false;

    @CommandLine.Option(names = {"--keep-temp-data"},
            description = "Do not remove temp folder in data directory after processing. For debug purposes only!")
    private boolean keepTempData = false;

    @CommandLine.Option(names = {"--parallel"},
            description = """
                Number of threads used for processing.
                Defaults to all available cpus minus 1.
                Dictates the number of output parquet files which will created.""")
    private int parallel = Runtime.getRuntime().availableProcessors() - 1;

    @CommandLine.Option(names = {"--country-file"},
            paramLabel = "path/to/countries.csv",
            description = """
                    Enrich osm-contributions with country-codes.
                    csv format (id;wkt):
                     id = country-code
                     wkt = geometry in wkt format
                    """)
    private Path countryFilePath;

    @CommandLine.Option(names = {"--changeset-db"},
            paramLabel = "jdbc:postgresql://HOST[:PORT]/DATABASE",
            description = """
                    Enrich osm-contributions with osm-changeset information.
                    Connection url to ohsome-planet changeset database.
                    Set connections parameters including credentials via jdbc:url "jdbc:postgresql://HOST[:PORT]/changesets?user=USER&password=PASSWORD"
                    or via environment variables (OHSOME_PLANET_DB_USER, OHSOME_PLANET_DB_PASSWORD, OHSOME_PLANET_DB_SCHEMA, OHSOME_PLANET_DB_POOLSIZE (default 10)).
                    """)
    private String changesetDbUrl = "";

    @CommandLine.Option(names = {"--replication-endpoint"},
            converter = UrlConverter.class,
            paramLabel = "https://planet.openstreetmap.org/replication/minute/",
            description = """
                    Url to replication endpoint from osm-planet-server (planet.openstreetmap.org) or alternatives.
                    Use this if you plan to derive replication updates later.
                    """)
    private URL replicationEndpoint;

    @CommandLine.Option(names = {"--filter-relation-tag-keys"},
            paramLabel = "highway,building,landuse",
            description = """
                    Filter osm relations with comma separated list of osm tag keys.
                    """)
    private String includeTags = "";

    @CommandLine.Option(names = {"-v"},
            description = "By default verbosity is set to warn, by repeating this flag the verbosity can be increased. -v=info, -vv=debug, -vvv=trace")
    boolean[] verbosity;


    @Override
    public Integer call() throws Exception {
        CliUtils.setVerbosity(verbosity);

        if (Files.exists(data)) {
            if (overwrite) {
                MoreFiles.deleteRecursively(data, ALLOW_INSECURE);
            } else {
                System.out.println("Directory already exists. To overwrite use --overwrite");
                System.exit(0);
            }
        }


        var tempDir = data.resolve("temp");
        Files.createDirectories(data);
        Files.createDirectories(tempDir);

        if (parquetData == null) {
            parquetData = data.resolve("contributions").toString();
        }

        var metricsPort = System.getProperty(OHSOME_PLANET_METRICS_PORT, System.getenv(OHSOME_PLANET_METRICS_PORT));
        try (var ignored = (metricsPort != null ) ?
                HTTPServer.builder().port(Integer.parseInt(metricsPort)).buildAndStart() : null ) {

            JvmMetrics.builder().register();

            var contributionsToParquet = new Contributions2Parquet(
                    pbfPath, data, parquetData, parallel,
                    changesetDbUrl, countryFilePath,
                    replicationEndpoint,
                    includeTags);

            if (!keepTempData) {
                MoreFiles.deleteRecursively(tempDir, ALLOW_INSECURE);
            }

            return contributionsToParquet.call();
        }
    }
}
