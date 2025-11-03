package org.heigit.ohsome.planet.cmd;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import org.heigit.ohsome.contributions.Contributions2Parquet;
import org.heigit.ohsome.planet.converter.UrlConverter;
import picocli.CommandLine;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "contributions", aliases = {"contribs"},
        mixinStandardHelpOptions = true,
        version = "ohsome-planet contribution 1.0.1", //TODO version should be automatically set see picocli.CommandLine.IVersionProvider
        description = "generates parquet files")
public class Contributions implements Callable<Integer> {

    @CommandLine.Option(names = {"--pbf"}, required = true)
    private Path pbfPath;

    @CommandLine.Option(names = {"--output", "-o"})
    private Path out = Path.of("out");

    @CommandLine.Option(names = {"--temp"})
    private Path temp;

    @CommandLine.Option(names = {"--overwrite"})
    private boolean overwrite = false;

    @CommandLine.Option(names = {"--parallel"}, description = "number of threads used for processing. Dictates the number of files which will created.")
    private int parallel = Runtime.getRuntime().availableProcessors() - 1;

    @CommandLine.Option(names = {"--country-file"})
    private Path countryFilePath;

    @CommandLine.Option(names = {"--changeset-db"}, description = "full jdbc:url to changesetmd database e.g. jdbc:postgresql://HOST[:PORT]/changesets?user=USER&password=PASSWORD")
    private String changesetDbUrl = "";


    @CommandLine.Option(names = {"--replication-workdir", "--workdir"})
    private Path replication;

    @CommandLine.Option(names = {"--replication-endpoint", "--endpoint" }, converter = UrlConverter.class)
    private URL replicationEndpoint;

    @CommandLine.Option(names = {"--debug"}, description = "Print debug information.")
    private boolean debug = false;

    @CommandLine.Option(names = {"--include-tags"}, description = "OSM keys of relations that should be built")
    private String includeTags = "";


    @Override
    public Integer call() throws Exception {
        if (Files.exists(out)) {
            if (overwrite) {
                MoreFiles.deleteRecursively(out, RecursiveDeleteOption.ALLOW_INSECURE);
            } else {
                System.out.println("Directory already exists. To overwrite use --overwrite");
                System.exit(0);
            }
        }

        if (temp == null) {
            temp = out;
        }

        if (replication == null) {
            replication = out.resolve("replication");
        }

        Files.createDirectories(out);
        Files.createDirectories(temp);
        Files.createDirectories(replication);

        var contributionsToParquet = new Contributions2Parquet(
                pbfPath, temp, out, parallel,
                changesetDbUrl, countryFilePath,
                replication, replicationEndpoint,
                includeTags, debug);

        return contributionsToParquet.call();
    }
}
