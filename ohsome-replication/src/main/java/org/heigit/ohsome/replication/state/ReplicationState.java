package org.heigit.ohsome.replication.state;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Instant;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;


public class ReplicationState {


    public static ReplicationState read(Path file) throws IOException {
        return read(Files.readAllBytes(file));
    }

    public static ReplicationState read(byte[] bytes) throws IOException {
        var props = new Properties();
        props.load(new ByteArrayInputStream(bytes));
        return new ReplicationState(props, "sequenceNumber", "timestamp", Instant::parse);
    }



    private final Instant timestamp;
    private final int sequenceNumber;

    public ReplicationState(Properties props, String sequenceKey, String timestampKey, Function<String, Instant> parser) {
        this.sequenceNumber = Integer.parseInt(props.getProperty(sequenceKey));
        this.timestamp = parser.apply(props.getProperty(timestampKey));
    }

    public ReplicationState(@JsonProperty("timestamp") Instant timestamp, @JsonProperty("sequenceNumber") int sequenceNumber) {
        this.timestamp = timestamp;
        this.sequenceNumber = sequenceNumber;
    }

    public static String sequenceNumberAsPath(int sequenceNumber) {
        DecimalFormatSymbols symbols = new DecimalFormatSymbols();
        symbols.setGroupingSeparator('/');
        DecimalFormat df = new DecimalFormat("000,000,000", symbols);
        return df.format(sequenceNumber);
    }



    @Override
    public String toString() {
        return "{sequenceNumber: " + sequenceNumber + ", timestamp: " + timestamp.toString() + "}";
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || getClass() != other.getClass()) return false;
        return Objects.equals(sequenceNumber, ((ReplicationState) other).sequenceNumber);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(sequenceNumber);
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public Integer getSequenceNumber() {
        return sequenceNumber;
    }
}
