package org.heigit.ohsome.replication;

import com.nimbusds.jose.util.Pair;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
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

    public String endpoint() {
        return targetUrl;
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

    public int getReplicationOffset() {
        return replicationOffset;
    }


    // Logic from https://github.com/osmcode/pyosmium/blob/b73e223b909cb82486ec09d4cee91215595beb96/src/osmium/replication/server.py#L324
    // # Copyright (C) 2025 Sarah Hoffmann <lonvia@denofr.de> and others.
    public ReplicationState findStartStateByTimestamp(Instant targetTimestamp, ReplicationState remoteState) throws IOException {
        // todo: returns one state before the actual one, right?
        var surroundingStates = getReplicationStatesAroundTargetTimestamp(remoteState, targetTimestamp);

        var lower = surroundingStates.getLeft();
        var upper = surroundingStates.getRight();

        if (lower.getTimestamp() == targetTimestamp) {
            return lower;
        }

        while (true) {
            var estimate = getRemoteState(estimateSeq(targetTimestamp, lower, upper));
            logger.debug("Next estimate for targetTimestamp {}: {}", targetTimestamp, estimate);

            if (estimate.getTimestamp().getEpochSecond() < targetTimestamp.getEpochSecond()) {
                lower = estimate;
            } else if (estimate.getTimestamp().getEpochSecond() == targetTimestamp.getEpochSecond()) {
                return estimate;
            } else {
                upper = estimate;
            }

            if (lower.getSequenceNumber() + 1 >= upper.getSequenceNumber()) {
                return lower;
            }
        }
    }


    private Pair<ReplicationState, ReplicationState> getReplicationStatesAroundTargetTimestamp(
            ReplicationState upperReplication,
            Instant targetTimestamp
    ) {
        var lowerReplication = estimateLowerReplicationState(upperReplication);
        logger.trace("Available replication: {}", lowerReplication);

        if (lowerReplication.getTimestamp().isBefore(targetTimestamp)) {
            logger.debug("Lower bound found: {}", lowerReplication);
            return Pair.of(lowerReplication, upperReplication);
        } else {
            if (lowerReplication.getSequenceNumber() == 0) {
                logger.warn("Server Replication {}, starts after given timestamp", targetUrl);
                return Pair.of(lowerReplication, upperReplication);
            }
            if (lowerReplication.getSequenceNumber() + 1 >= upperReplication.getSequenceNumber()) {
                logger.debug("Perfect fit found already!");
                return Pair.of(lowerReplication, upperReplication);
            }
            return getReplicationStatesAroundTargetTimestamp(lowerReplication, targetTimestamp);
        }
    }

    private ReplicationState estimateLowerReplicationState(ReplicationState upperReplication) {
        var lowerReplication = new ReplicationState(null, 0);

        while (lowerReplication.getTimestamp() == null) {
            try {
                lowerReplication = getRemoteState(lowerReplication.getSequenceNumber() + replicationOffset);
            } catch (IOException e) {
                var seqNumberCloserToUpperSeq = getNextProbingPoint(upperReplication, lowerReplication);
                lowerReplication = new ReplicationState(null, seqNumberCloserToUpperSeq);
            }
        }
        return lowerReplication;
    }

    private static int getNextProbingPoint(ReplicationState upperReplication, ReplicationState lowerReplication) {
        return (upperReplication.getSequenceNumber() + lowerReplication.getSequenceNumber()) / 2;
    }

    private int estimateSeq(Instant targetTimestamp, ReplicationState lower, ReplicationState upper) {
        var secsToTarget = Duration.between(lower.getTimestamp(), targetTimestamp).toSeconds();

        var secsBetweenSurrounding = Duration.between(lower.getTimestamp(), upper.getTimestamp()).toSeconds();
        var numsBetweenSurrounding = upper.getSequenceNumber() - lower.getSequenceNumber();

        var baseSplitSeq = lower.getSequenceNumber()
                + (int) Math.ceil((double) (secsToTarget * numsBetweenSurrounding) / secsBetweenSurrounding);

        return Math.min(baseSplitSeq, upper.getSequenceNumber() - 1) + replicationOffset;
    }
}
