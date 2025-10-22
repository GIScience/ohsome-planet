package org.heigit.ohsome.replication.state;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static java.net.URI.create;

public abstract class AbstractStateManager<T> {
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

    protected abstract void initializeLocalState() throws IOException;

    protected abstract void updateLocalState(ReplicationState state) throws IOException;

    protected abstract Iterator<T> getParser(InputStream input) throws Exception;

    protected InputStream getFileStream(URL url) throws IOException {
        var connection = url.openConnection();
        connection.setReadTimeout(10 * 60 * 1000); // timeout 10 minutes
        connection.setConnectTimeout(10 * 60 * 1000); // timeout 10 minutes
        return connection.getInputStream();
    }

    public ReplicationState fetchRemoteState() throws IOException {
        var input = getFileStream(create(this.targetUrl + topLevelFile).toURL());
        var props = new Properties();
        props.load(input);
        this.remoteState = new ReplicationState(props, sequenceKey, timestampKey, this::timestampParser);
        return this.remoteState;
    }

    public ReplicationState getRemoteState() {
        return this.remoteState;
    }

    public ReplicationState getRemoteReplication(Integer sequenceNumber) {
        try {
            var input = getFileStream(create(this.targetUrl + ReplicationState.sequenceNumberAsPath(sequenceNumber) + ".state.txt").toURL());
            var props = new Properties();
            props.load(input);
            return new ReplicationState(props, sequenceKey, timestampKey, this::timestampParser);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected InputStream getReplicationFile(String replicationPath) {
        try {
            return getFileStream(create(this.targetUrl + replicationPath + this.replicationFileName).toURL());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected List<T> fetchReplicationBatch(String replicationPath) throws Exception {
        try (var gzipStream = new GZIPInputStream(getReplicationFile(replicationPath))) {
            return parse(gzipStream);
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

    public ReplicationState estimateLocalReplicationState(Instant targetTimestamp, ReplicationState remoteState) {
        var replicationMap = new HashMap<Integer, ReplicationState>();
        var targetMinute = targetTimestamp.truncatedTo(ChronoUnit.MINUTES);

        while (!remoteState.getTimestamp().truncatedTo(ChronoUnit.MINUTES).equals(targetMinute)) {
            var minutes = Duration.between(targetMinute, remoteState.getTimestamp().truncatedTo(ChronoUnit.MINUTES)).toMinutes();
            remoteState = getRemoteReplication(remoteState.getSequenceNumber() - Math.toIntExact(minutes) + replicationOffset);

            if (replicationMap.putIfAbsent(remoteState.getSequenceNumber(), remoteState) != null) {
                return getRemoteStateInCaseOfLoop(targetTimestamp, replicationMap);
            }
        }
        return targetTimestamp.isBefore(remoteState.getTimestamp())
                ? remoteState
                : getRemoteReplication(remoteState.getSequenceNumber() + 1 + replicationOffset);
    }

    private ReplicationState getRemoteStateInCaseOfLoop(Instant targetTimestamp, HashMap<Integer, ReplicationState> replicationMap) {
        var closestReplicationState = replicationMap.values().stream()
                .filter(rs -> rs.getTimestamp().isBefore(targetTimestamp))
                .max(Comparator.comparing(ReplicationState::getTimestamp))
                .orElseThrow(); // can not happen since we cannot loop if we are never below timestamp

        ReplicationState previous;
        do {
            previous = closestReplicationState;
            closestReplicationState = getRemoteReplication(previous.getSequenceNumber() + 1 + replicationOffset);
        } while (closestReplicationState.getTimestamp().isBefore(targetTimestamp));
        return previous;
    }

    public ReplicationState getLocalState() {
        return localState;
    }
}
