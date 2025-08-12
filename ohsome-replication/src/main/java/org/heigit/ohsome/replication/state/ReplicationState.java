package org.heigit.ohsome.replication.state;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Instant;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;


public class ReplicationState {
    public Instant timestamp;
    public Integer sequenceNumber;

    ReplicationState(Properties props, String sequenceKey, String timestampKey, Function<String, Instant> parser) {
        this.sequenceNumber = Integer.parseInt(props.getProperty(sequenceKey));
        this.timestamp = parser.apply(props.getProperty(timestampKey));
    }

    public ReplicationState(Instant timestamp, Integer sequenceNumber){
        this.timestamp = timestamp;
        this.sequenceNumber = sequenceNumber;
    }

    public static String sequenceNumberAsPath(int sequenceNumber){
        DecimalFormatSymbols symbols = new DecimalFormatSymbols();
        symbols.setGroupingSeparator('/');
        DecimalFormat df = new DecimalFormat("000,000,000", symbols);
        return df.format(sequenceNumber);
    }

    public boolean equals(ReplicationState other) {
        return Objects.equals(sequenceNumber, other.sequenceNumber);
    }
}
