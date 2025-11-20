package org.heigit.ohsome.replication;

import org.heigit.ohsome.osm.OSMEntity;
import org.heigit.ohsome.osm.xml.osc.OscParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;

import static java.net.URI.create;
import static org.heigit.ohsome.osm.changesets.OSMChangesets.OSMChangeset;
import static org.heigit.ohsome.osm.changesets.OSMChangesets.readChangesets;

public class Server<T> {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    public static Server<OSMChangeset> osmChangesetServer(String endpoint) {
        return new Server<>(
                endpoint,
                "state.yaml",
                "sequence",
                "last_run",
                ".osm.gz",
                1,
                Server::parseTimestamp,
                input -> readChangesets(input).iterator()
        );
    }

    public static final Server<OSMChangeset> OSM_CHANGESET_SERVER = osmChangesetServer("https://planet.openstreetmap.org/replication/changesets/");


    static Instant parseTimestamp(String timestamp) {
        return OffsetDateTime.parse(timestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS XXX")).toInstant();
    }

    public static Server<OSMEntity> osmEntityServer(
            String targetURL
    ) {
        return new Server<>(
                targetURL,
                "state.txt",
                "sequenceNumber",
                "timestamp",
                ".osc.gz",
                0,
                Instant::parse,
                OscParser::new
        );
    }

    @FunctionalInterface
    public interface ParserFunction<T> {
        Iterator<T> parse(InputStream input) throws Exception;
    }


    protected final String targetUrl;
    protected final String topLevelFile;
    protected final String sequenceKey;
    protected final String timestampKey;
    protected final String replicationFileName;
    protected final Integer replicationOffset;
    private final Function<String, Instant> timestampParser;
    private final ParserFunction<T> parser;

    Server(
            String targetURL,
            String topLevelFile,
            String sequenceKey,
            String timestampKey,
            String replicationFileName,
            Integer replicationOffset,
            Function<String, Instant> timestampParser,
            ParserFunction<T> parser
    ) {
        targetUrl = targetURL;
        this.topLevelFile = topLevelFile;
        this.sequenceKey = sequenceKey;
        this.timestampKey = timestampKey;
        this.replicationFileName = replicationFileName;
        this.replicationOffset = replicationOffset;
        this.timestampParser = timestampParser;
        this.parser = parser;
    }

    public static InputStream getFileStream(URL url) throws IOException {
        var connection = url.openConnection();
        connection.setReadTimeout(10 * 60 * 1000); // timeout 10 minutes
        connection.setConnectTimeout(10 * 60 * 1000); // timeout 10 minutes
        return connection.getInputStream();
    }

    public static byte[] getFile(URL url) throws IOException {
        return getFileStream(url).readAllBytes();
    }


    protected byte[] getFile(String replicationPath) throws IOException {
        return getFile(create(this.targetUrl + replicationPath + this.replicationFileName).toURL());
    }


    public ReplicationState getLatestRemoteState() throws IOException {
        return getRemoteState(create(this.targetUrl + topLevelFile).toURL());
    }


    public ReplicationState getRemoteState(int sequenceNumber) throws IOException {
        var url = create(this.targetUrl + ReplicationState.sequenceNumberAsPath(sequenceNumber) + ".state.txt").toURL();
        return getRemoteState(url);
    }

    public ReplicationState getRemoteState(URL url) throws IOException {
        var input = getFileStream(url);
        var props = new Properties();
        props.load(input);
        return new ReplicationState(props, sequenceKey, timestampKey, timestampParser);
    }

    public byte[] getReplicationFile(int sequenceNumber) throws IOException {
        return new GZIPInputStream(getFileStream(create(this.targetUrl + ReplicationState.sequenceNumberAsPath(sequenceNumber) + this.replicationFileName).toURL())).readAllBytes();
    }

    public Iterator<T> getElements(ReplicationState state) throws Exception {
        return getElements(state.getSequenceNumber());
    }

    public Iterator<T> getElements(int sequenceNumber) throws Exception {
        var file = getReplicationFile(sequenceNumber);
        return parser.parse(new ByteArrayInputStream(file));
    }

    private Iterator<T> parseFile(byte[] file) throws Exception {
        return parser.parse(new ByteArrayInputStream(file));
    }

    public List<T> parseToList(byte[] file) throws Exception {
        var list = new ArrayList<T>();
        parseFile(file).forEachRemaining(list::add);
        return list;
    }

    public List<T> getListElements(int sequenceNumber) throws Exception {
        var list = new ArrayList<T>();
        getElements(sequenceNumber).forEachRemaining(list::add);
        return list;
    }

    public int getReplicationOffset(){
        return replicationOffset;
    }

    public ReplicationState estimateLocalReplicationState(Instant lastTimestampChangeset, ReplicationState remoteState)
            throws IOException {
        var replicationMap = new HashMap<Integer, ReplicationState>();
        var targetMinute = lastTimestampChangeset.truncatedTo(ChronoUnit.MINUTES);
        logger.debug("Trying to estimate Replication ID for {}", lastTimestampChangeset);

        while (!remoteState.getTimestamp().truncatedTo(ChronoUnit.MINUTES).equals(targetMinute)) {
            var minutes = Duration.between(targetMinute, remoteState.getTimestamp().truncatedTo(ChronoUnit.MINUTES)).toMinutes();
            remoteState = getRemoteState(Math.max(remoteState.getSequenceNumber() - Math.toIntExact(minutes) + replicationOffset, 1));
            logger.debug("Found remote state state {}", remoteState);

            if (replicationMap.putIfAbsent(remoteState.getSequenceNumber(), remoteState) != null) {
                logger.debug("Loop detected, trying to brute force replication date");
                return getRemoteStateInCaseOfLoop(lastTimestampChangeset, replicationMap);
            }
        }
        return lastTimestampChangeset.isBefore(remoteState.getTimestamp())
                ? remoteState
                : getRemoteState(remoteState.getSequenceNumber() + 1 + replicationOffset);
    }

    private ReplicationState getRemoteStateInCaseOfLoop(Instant targetTimestamp, Map<Integer, ReplicationState> replicationMap)
            throws IOException {
        var closestReplicationState = replicationMap.values().stream()
                .filter(rs -> rs.getTimestamp().isBefore(targetTimestamp))
                .max(Comparator.comparing(ReplicationState::getTimestamp))
                .orElseThrow(); // can not happen since we cannot loop if we are never below timestamp
        logger.debug("Starting brute force at {}", closestReplicationState);
        ReplicationState previous;
        do {
            previous = closestReplicationState;
            closestReplicationState = getRemoteState(previous.getSequenceNumber() + 1 + replicationOffset);
            logger.debug("Then comes {}", closestReplicationState);
        } while (closestReplicationState.getTimestamp().isBefore(targetTimestamp));
        return previous;
    }

}
