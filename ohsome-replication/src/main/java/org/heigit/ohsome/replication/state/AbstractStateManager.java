package org.heigit.ohsome.replication.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static java.net.URI.create;

public abstract class AbstractStateManager<T> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractStateManager.class);

    protected final String targetUrl;
    protected final String topLevelFile;
    protected final String sequenceKey;
    protected final String timestampKey;
    protected final String replicationFileName;
    protected final Integer replicationOffset;

    protected ReplicationState localState;
    protected ReplicationState remoteState;


    AbstractStateManager(String targetURL, String topLevelFile, String sequenceKey, String timestampKey, String replicationFileName, Integer replicationOffset) {
        targetUrl = targetURL;
        this.topLevelFile = topLevelFile;
        this.sequenceKey = sequenceKey;
        this.timestampKey = timestampKey;
        this.replicationFileName = replicationFileName;
        this.replicationOffset = replicationOffset;
    }

    protected abstract Instant timestampParser(String timestamp);

    protected abstract void initializeLocalState() throws Exception;

    protected abstract void updateLocalState(ReplicationState state) throws IOException, SQLException;

    protected abstract Iterator<T> getParser(InputStream input) throws Exception;

    protected InputStream getFileStream(URL url) throws IOException {
        var connection = url.openConnection();
        connection.setReadTimeout(10 * 60 * 1000); // timeout 10 minutes
        connection.setConnectTimeout(10 * 60 * 1000); // timeout 10 minutes
        return connection.getInputStream();
    }

    protected byte[] getFile(URL url) throws IOException {
        return getFileStream(url).readAllBytes();
    }

    protected byte[] getFile(String url) throws IOException {
        return getFile(create(this.targetUrl + url + this.replicationFileName).toURL());
    }

    public ReplicationState fetchRemoteState() throws IOException {
        var input = getFileStream(create(this.targetUrl + topLevelFile).toURL());
        var props = new Properties();
        props.load(input);
        return setRemoteState(new ReplicationState(props, sequenceKey, timestampKey, this::timestampParser));
    }

    public ReplicationState setRemoteState(ReplicationState remoteState) {
        this.remoteState = remoteState;
        return this.remoteState;
    }

    public ReplicationState getRemoteState() {
        return this.remoteState;
    }

    public ReplicationState getRemoteReplication(int sequenceNumber) throws IOException {
        var input = getFileStream(create(this.targetUrl + ReplicationState.sequenceNumberAsPath(sequenceNumber) + ".state.txt").toURL());
        var props = new Properties();
        props.load(input);
        return new ReplicationState(props, sequenceKey, timestampKey, this::timestampParser);
    }

    private InputStream getReplicationFile(String replicationPath) throws IOException {
        return new GZIPInputStream(getFileStream(create(this.targetUrl + replicationPath + this.replicationFileName).toURL()));
    }

    protected List<T> fetchReplicationBatch(ReplicationState state) throws Exception {
        return fetchReplicationBatch(ReplicationState.sequenceNumberAsPath(state.getSequenceNumber()));
    }

    protected List<T> parseGZIP(byte[] file) throws Exception {
        try (var input = new GZIPInputStream(new ByteArrayInputStream(file))) {
            return parse(input);
        }
    }

    protected List<T> parse(byte[] file) throws Exception {
        try (var input = new ByteArrayInputStream(file)) {
            return parse(input);
        }
    }

    protected List<T> fetchReplicationBatch(String replicationPath) throws Exception {
        try (var input = getReplicationFile(replicationPath)) {
            return parse(input);
        }
    }

    protected List<T> parse(InputStream input) throws Exception {
        var xmlReader = getParser(input);
        var elements = new ArrayList<T>();
        while (xmlReader.hasNext()) {
            elements.add(xmlReader.next());
        }
        return elements;
    }

    public ReplicationState estimateLocalReplicationState(Instant lastTimestampChangeset, ReplicationState remoteState)
            throws IOException {
        var replicationMap = new HashMap<Integer, ReplicationState>();
        var targetMinute = lastTimestampChangeset.truncatedTo(ChronoUnit.MINUTES);
        logger.debug("Trying to estimate Replication ID for {}", lastTimestampChangeset);

        while (!remoteState.getTimestamp().truncatedTo(ChronoUnit.MINUTES).equals(targetMinute)) {
            var minutes = Duration.between(targetMinute, remoteState.getTimestamp().truncatedTo(ChronoUnit.MINUTES)).toMinutes();
            remoteState = getRemoteReplication(Math.max(remoteState.getSequenceNumber() - Math.toIntExact(minutes) + replicationOffset, 1));
            logger.debug("Found remote state state {}", remoteState);

            if (replicationMap.putIfAbsent(remoteState.getSequenceNumber(), remoteState) != null) {
                logger.debug("Loop detected, trying to brute force replication date");
                return getRemoteStateInCaseOfLoop(lastTimestampChangeset, replicationMap);
            }
        }
        return lastTimestampChangeset.isBefore(remoteState.getTimestamp())
                ? remoteState
                : getRemoteReplication(remoteState.getSequenceNumber() + 1 + replicationOffset);
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
            closestReplicationState = getRemoteReplication(previous.getSequenceNumber() + 1 + replicationOffset);
            logger.debug("Then comes {}", closestReplicationState);
        } while (closestReplicationState.getTimestamp().isBefore(targetTimestamp));
        return previous;
    }

    public ReplicationState getLocalState() {
        return localState;
    }
}
