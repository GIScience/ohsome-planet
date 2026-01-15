package org.heigit.ohsome.planet.cmd;

import org.heigit.ohsome.contributions.Contributions2Parquet;
import org.heigit.ohsome.planet.converter.UrlConverter;
import org.heigit.ohsome.planet.utils.CliUtils;
import org.heigit.ohsome.planet.utils.ManifestVersionProvider;
import picocli.CommandLine;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "contributions", aliases = {"contribs"},
        mixinStandardHelpOptions = true,
        versionProvider = ManifestVersionProvider.class,
        description = "generates parquet files")
public class Contributions implements Callable<Integer> {

    @CommandLine.Option(names = {"--pbf"}, required = true)
    private Path pbfPath;

    @CommandLine.Option(names = {"--data"}, required = true)
    private Path data;

    @CommandLine.Option(names = {"--parquet-data"})
    private String parquetData;

    @CommandLine.Option(names = {"--overwrite"})
    private boolean overwrite = false;

    @CommandLine.Option(names = {"--keep-temp-data"}, required = false)
    private boolean keepTempData = false;

    @CommandLine.Option(names = {"--parallel"}, description = "number of threads used for processing. Dictates the number of files which will created.")
    private int parallel = Runtime.getRuntime().availableProcessors() - 1;

    @CommandLine.Option(names = {"--country-file"})
    private Path countryFilePath;

    @CommandLine.Option(names = {"--changeset-db"}, description = "full jdbc:url to changesetmd database e.g. jdbc:postgresql://HOST[:PORT]/changesets?user=USER&password=PASSWORD")
    private String changesetDbUrl = "";

    @CommandLine.Option(names = {"--replication-endpoint", "--endpoint" },
            converter = UrlConverter.class,
            description = "url to replication endpoint e.g. https://planet.openstreetmap.org/replication/minute/")
    private URL replicationEndpoint;

    @CommandLine.Option(names = {"--include-tags"}, description = "OSM keys of relations that should be built")
    private String includeTags = "";

    @CommandLine.Option(names = {"-v", "--verbose"}, description = "By default verbosity is set to warn, by repeating this flag the verbosity can be increased. -v=info, -vv=debug, -vvv=trace")
    boolean[] verbosity;


    @Override
    public Integer call() throws Exception {
        CliUtils.setVerbosity(verbosity);

        Files.createDirectories(data);

        if (parquetData == null) {
            parquetData = data.resolve("contributions").toString();
        }

        var contributionsToParquet = new Contributions2Parquet(
                pbfPath, data, parquetData, parallel,
                changesetDbUrl, countryFilePath,
                replicationEndpoint,
                includeTags);

        return contributionsToParquet.call();
    }
}
