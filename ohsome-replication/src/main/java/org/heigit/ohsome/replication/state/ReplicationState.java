package org.heigit.ohsome.replication.state;

import java.time.Instant;
import java.util.Properties;
import java.util.function.Function;


public class ReplicationState {
    public Instant timestamp;
    public int sequenceNumber;

    ReplicationState(Properties props, String sequenceKey, String timestampKey, Function<String, Instant> parser) {
        this.sequenceNumber = Integer.parseInt(props.getProperty(sequenceKey));
        this.timestamp = parser.apply(props.getProperty(timestampKey));
    }
}
