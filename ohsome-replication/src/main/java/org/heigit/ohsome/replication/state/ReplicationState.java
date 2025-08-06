package org.heigit.ohsome.replication.state;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Instant;
import java.util.Properties;
import java.util.function.Function;


public class ReplicationState {
    public Instant timestamp;
    public Integer sequenceNumber;

    ReplicationState(Properties props, String sequenceKey, String timestampKey, Function<String, Instant> parser) {
        this.sequenceNumber = Integer.parseInt(props.getProperty(sequenceKey));
        this.timestamp = parser.apply(props.getProperty(timestampKey));
    }

    ReplicationState(Instant timestamp, Integer sequenceNumber){
        this.timestamp = timestamp;
        this.sequenceNumber = sequenceNumber;
    }

    String sequenceNumberAsPath(){
        DecimalFormatSymbols symbols = new DecimalFormatSymbols();
        symbols.setGroupingSeparator('/');
        DecimalFormat df = new DecimalFormat("000,000,000", symbols);
        return df.format(this.sequenceNumber);
    }
}
