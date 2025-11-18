package org.heigit.ohsome.replication;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

public class ReplicationServer {
    public static Properties state(URL replicationEndpoint) throws IOException {
        var rs = new ReplicationServer(replicationEndpoint);
        return rs.state();
    }

    public static final DecimalFormat SEQUENCE_FORMAT;

    static {
        var symbols = new DecimalFormatSymbols();
        symbols.setGroupingSeparator('/');
        SEQUENCE_FORMAT = new DecimalFormat("000,000,000", symbols);
    }

    public static Properties tryToFindStartFromTimestamp(URL endpoint, Instant timestamp) {
        if (endpoint != null) {
            var rs = new ReplicationServer(endpoint);
            try {
                var start = rs.tryFindStartSequenceByTimestamp(timestamp);
                var state = rs.state(start);
                state.setProperty("endpoint", endpoint.toString());
                return state;
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        var props = new Properties();
        props.setProperty("timestamp", timestamp.toString());
        props.setProperty("sequenceNumber", "-1");
        return props;
    }

    private final String endpoint;


    public ReplicationServer(URL endpoint) {
        this.endpoint = endpoint.toString();
    }

    // todo: decide on this vs whats in estimateLocalReplicationState
    public int tryFindStartSequenceByTimestamp(Instant timestamp) throws IOException {
        var latestState = state();
        var latestTimestamp = timestamp(latestState);
        var latestSeq = sequence(latestState);

        var upperTimestamp = latestTimestamp;
        var upperSeq = latestSeq;


        var lowerTimestamp = (Instant) null;
        var lowerSeq = 0;

        // find lower
        while (lowerTimestamp == null) {
            try {
                var state = state(lowerSeq);
                lowerTimestamp = timestamp(state);
                lowerSeq = sequence(state);

                if (lowerTimestamp.getEpochSecond() >= timestamp.getEpochSecond()) {
                    if (lowerSeq == 0) {
                        return 0;
                    }
                    if (lowerSeq + 1 >= upperSeq) {
                        return lowerSeq;
                    }
                    upperTimestamp = lowerTimestamp;
                    upperSeq = lowerSeq;
                    lowerTimestamp = null;
                    lowerSeq = 0;
                }

            } catch (IOException e) {
//                System.out.println("miss " + lowerSeq);
                lowerSeq = (upperSeq + lowerSeq) / 2;
            }
        }
//        System.out.println("lowerTimestamp = " + lowerTimestamp);
//        System.out.println("lowerSeq = " + lowerSeq);

        while (true) {
            var goal = Duration.between(lowerTimestamp, timestamp).toSeconds();
            if (goal == 0) {
                return lowerSeq;
            }
            var duration = Duration.between(lowerTimestamp, upperTimestamp).toSeconds();
            var seqs = upperSeq - lowerSeq;
            var baseSplitId = lowerSeq + (int) Math.ceil((double) (goal * seqs) / duration);
            if (baseSplitId >= upperSeq) {
                baseSplitId = upperSeq - 1;
            }
            var split = (Properties) null;
            try {
//                System.out.println("baseSplitId = " + baseSplitId);
                split = state(baseSplitId);
            } catch (IOException e) {
                // todo missing state files!
                throw e;
            }

            var splitTimestamp = timestamp(split);
            var splitSeq = sequence(split);
            if (splitTimestamp.getEpochSecond() < timestamp.getEpochSecond()) {
                lowerTimestamp = splitTimestamp;
                lowerSeq = splitSeq;
            } else if (splitTimestamp.getEpochSecond() == timestamp.getEpochSecond()) {
                return splitSeq;
            } else {
                upperTimestamp = splitTimestamp;
                upperSeq = splitSeq;
            }
            if (lowerSeq + 1 >= upperSeq) {
                return lowerSeq;
            }
        }
    }

    public Properties state() throws IOException {
        return state("state.txt");
    }

    public Properties state(int sequence) throws IOException {
        return state(SEQUENCE_FORMAT.format(sequence) + ".state.txt");
    }

    public Properties state(String path) throws IOException {
        try (var input = openConnection(URI.create("%s/%s".formatted(endpoint, path)).toURL())) {
            var props = new Properties();
            props.load(new ByteArrayInputStream(input.readAllBytes()));
            return props;
        }
    }

    public static Instant timestamp(Properties props) {
        return Instant.parse(props.getProperty("timestamp", Instant.EPOCH.toString()));
    }

    public static int sequence(Properties props) {
        return Integer.parseInt(props.getProperty("sequenceNumber", "-1"));
    }

    private static InputStream openConnection(URL url) throws IOException {
        var connection = url.openConnection();
        connection.setReadTimeout(10 * 60 * 1000); // timeout 10 minutes
        connection.setConnectTimeout(10 * 60 * 1000); // timeout 10 minutes
        return connection.getInputStream();
    }

}
