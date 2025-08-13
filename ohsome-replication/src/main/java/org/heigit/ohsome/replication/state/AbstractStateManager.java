package org.heigit.ohsome.replication.state;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;

import static java.net.URI.create;

public abstract class AbstractStateManager<T> {
    protected final String TARGET_URL;
    protected final String TOP_LEVEL_FILE;
    protected final String SEQUENCE_KEY;
    protected final String TIMESTAMP_KEY;
    protected final String REPLICATION_FILE_NAME;

    public ReplicationState localState;
    public ReplicationState remoteState;


    AbstractStateManager(String targetURL, String topLevelFile, String sequenceKey, String timestampKey, String replicationFileName) {
        TARGET_URL = targetURL;
        TOP_LEVEL_FILE = topLevelFile;
        SEQUENCE_KEY = sequenceKey;
        TIMESTAMP_KEY = timestampKey;
        REPLICATION_FILE_NAME = replicationFileName;
    }

    abstract protected Instant timestampParser(String timestamp);

    abstract protected ReplicationState getLocalState();

    abstract protected void updateLocalState(ReplicationState state);

    abstract protected Iterator<T> getParser(InputStream input);

    protected InputStream getFileStream(URL url) throws IOException {
        var connection = url.openConnection();
        connection.setReadTimeout(10 * 60 * 1000); // timeout 10 minutes
        connection.setConnectTimeout(10 * 60 * 1000); // timeout 10 minutes
        return connection.getInputStream();
    }

    public ReplicationState getRemoteState() {
        try {
            var input = getFileStream(create(this.TARGET_URL + TOP_LEVEL_FILE).toURL());
            var props = new Properties();
            props.load(input);
            this.remoteState = new ReplicationState(props, SEQUENCE_KEY, TIMESTAMP_KEY, this::timestampParser);
            return this.remoteState;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public InputStream getReplicationFile(String replicationPath) {
        try {
            return getFileStream(create(this.TARGET_URL + replicationPath + this.REPLICATION_FILE_NAME).toURL());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<T> fetchReplicationBatch(String replicationPath) {
        try (var gzipStream = new GZIPInputStream(getReplicationFile(replicationPath))) {
            return parse(gzipStream);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    protected List<T> parse(InputStream input) {
        var xmlReader = getParser(input);
        var elements = new ArrayList<T>();
        while (xmlReader.hasNext()) {
            elements.add(xmlReader.next());
        }
        return elements;
    }

    protected void parseAndProcessBatch(InputStream input, Consumer<List<T>> processor, Integer batchSize) {
        var xmlReader = getParser(input);
        var elements = new ArrayList<T>();
        var i = 0;
        while (xmlReader.hasNext()) {
            elements.add(xmlReader.next());
            ++i;
            if (i == batchSize) {
                processor.accept(elements);
                elements.clear();
                i = 0;
            }
        }
        if (!elements.isEmpty()) {
            processor.accept(elements);
        }
    }

}
