package org.heigit.ohsome.replication.state;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Instant;
import java.util.Properties;

import static java.net.URI.create;

public abstract class AbstractStateManager {
    protected final String TARGET_URL;
    protected final String TOP_LEVEL_FILE;
    protected final String SEQUENCE_KEY;
    protected final String TIMESTAMP_KEY;

    public ReplicationState localState;
    public ReplicationState remoteState;


    AbstractStateManager(String targetURL, String topLevelFile, String sequenceKey, String timestampKey) {
        TARGET_URL = targetURL;
        TOP_LEVEL_FILE = topLevelFile;
        SEQUENCE_KEY = sequenceKey;
        TIMESTAMP_KEY = timestampKey;
    }

    abstract protected Instant timestampParser(String timestamp);


    private static InputStream getFileStream(URL url) throws IOException {
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

    abstract protected ReplicationState getLocalState();

}
