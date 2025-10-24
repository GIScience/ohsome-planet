package org.heigit.ohsome.replication.state;

import static reactor.core.publisher.Mono.fromCallable;
import static reactor.core.scheduler.Schedulers.boundedElastic;

import org.heigit.ohsome.contributions.avro.Contrib;
import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.parquet.ParquetUtil;
import org.heigit.ohsome.replication.parser.OscParser;
import org.heigit.ohsome.replication.processor.ContributionsProcessor;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Iterator;
import java.util.Properties;

public class ContributionStateManager extends AbstractStateManager<OSMEntity> {

    public static ContributionStateManager openManager(String endpoint, Path directory) throws IOException {
        return openManager(endpoint, directory, Path.of("out"));
    }
  public static ContributionStateManager openManager(String endpoint, Path directory, Path out) throws IOException {
    var localStatePath = directory.resolve("state.txt");
    var localState = loadLocalState(localStatePath);
    return new ContributionStateManager(endpoint, directory, localState, out);
  }

  public static final String PLANET_OSM_MINUTELY = "https://planet.openstreetmap.org/replication/minute/";
  public static final String PLANET_OSM_HOURLY = "https://planet.openstreetmap.org/replication/hour/";
  private final String endpoint;
  private final Path directory;
  private final Path localStatePath;
  private final Path out;


  public ContributionStateManager(String endpoint, Path directory) throws IOException {
    this(endpoint, directory, null, null);
  }

  public ContributionStateManager(String endpoint, Path directory, ReplicationState localState, Path out)
      throws IOException {
    super(endpoint + "/", "state.txt", "sequenceNumber", "timestamp", ".osc.gz", 0);
    this.endpoint = endpoint;
    this.directory = directory;
    Files.createDirectories(directory);
    this.out = out;
    Files.createDirectories(out);
    this.localStatePath = directory.resolve("state.txt");
    this.localState = localState;
  }

  @Override
  protected Instant timestampParser(String timestamp) {
    return Instant.parse(timestamp);
  }

  @Override
  public void initializeLocalState() throws IOException {
    if (Files.exists(localStatePath)) {
      var props = new Properties();
      try (var in = Files.newInputStream(localStatePath)) {
        props.load(in);
      }
      localState = new ReplicationState(props, "sequenceNumber", "timestamp", Instant::parse);
    }
  }

  public static ReplicationState loadLocalState(Path localStatePath) throws IOException {
    if (Files.exists(localStatePath)) {
      return ReplicationState.read(localStatePath);
    }
    return null;
  }

  @Override
  protected void updateLocalState(ReplicationState state) throws IOException {
    var props = new Properties();
    props.put("timestamp", state.getTimestamp().toString());
    props.put("sequenceNumber", Integer.toString(state.getSequenceNumber()));
    props.put("endpoint", endpoint);
    try (var out = Files.newOutputStream(localStatePath)) {
      props.store(out, null);
    }
    localState = state;
  }

  public void updateTowardsRemoteState(ContributionsProcessor processor) throws Exception {
    var local = localState.getSequenceNumber();
    var remote = remoteState.getSequenceNumber();
    var steps = remote - local;
    var statesUpdated = Flux.range(local + 1, steps)
        .flatMapSequential(next -> fromCallable(() -> getRemoteReplication(next)).subscribeOn(boundedElastic()), 8)
        .concatMap(state -> fromCallable(() -> process(state, processor)))
        .count()
        .blockOptional()
        .orElseThrow();
    // var entities = fetchReplicationBatch(ReplicationState.sequenceNumberAsPath(nextSequenceNumber));
  }

  private int process(ReplicationState state, ContributionsProcessor processor) throws IOException {
      var path =  state.getSequenceNumberPath(out);
      path = path.getParent().resolve(path.getFileName() + ".parquet");
      try (var writer = ParquetUtil.openWriter(path, Contrib.getClassSchema(), builder -> {})){

      }
      updateLocalState(state);
      return state.getSequenceNumber();
  }

  @Override
  protected Iterator<OSMEntity> getParser(InputStream input) throws Exception {
    return new OscParser(input);
  }
}
