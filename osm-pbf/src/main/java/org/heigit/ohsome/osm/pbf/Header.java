package org.heigit.ohsome.osm.pbf;

import org.heigit.ohsome.util.io.Input;

import java.util.HashSet;
import java.util.Set;

public class Header implements ProtoZero.Message {

    public static class BBox implements ProtoZero.Message {
        private double left;
        private double right;
        private double top;
        private double bottom;

        @Override
        public boolean decode(Input input, int tag) {
            switch(tag) {
                case 8 -> left = input.readS64() / 1.0E9;
                case 16 -> right = input.readS64() / 1.0E9;
                case 24 -> top = input.readS64() / 1.0E9;
                case 32 -> bottom = input.readS64() / 1.0E9;
                default -> {
                    return false;
                }
            }
            return true;
        }

        public double left() {
            return left;
        }

        public double right() {
            return right;
        }

        public double top() {
            return top;
        }

        public double bottom() {
            return bottom;
        }

        @Override
        public String toString() {
            return "BBox{" +
                   "left=" + left() +
                   ", right=" + right() +
                   ", top=" + top() +
                   ", bottom=" + bottom() +
                   '}';
        }
    }

    public static final String HISTORICAL_INFORMATION = "HistoricalInformation";
    // required features
    //   "OsmSchema-V0.6" — File contains data with the OSM v0.6 schema.
    //   "DenseNodes" — File contains dense nodes and dense info.
    //   "HistoricalInformation" — File contains historical OSM data.

    // optional featuers:
    //   "Has_Metadata" – The file contains author and timestamp metadata.
    //   "Sort.Type_then_ID" – Entities are sorted by type then ID.
    //   "Sort.Geographic" – Entities are in some form of geometric sort. (presently unused)
    //   "timestamp=2011-10-16T15:45:00Z" – Interim solution for storing a file timestamp. Please use osmosis_replication_timestamp instead.
    //   "LocationsOnWays" — File has lat/lon values on each way. See Ways and Relations below.



    private String writingProgram;
    private String source;

    private BBox bbox;
    private final Set<String> requiredFeatures = new HashSet<>();
    private final Set<String> optionalFeatures = new HashSet<>();

    private long replicationTimestamp;
    private long replicationSequenceNumber;
    private String replicationBaseUrl;

    @Override
    public boolean decode(Input input, int tag) {
        switch (tag) {
            case 10 -> this.bbox = ProtoZero.decodeMessage(input.readBuffer(), BBox::new);
            case 34 -> this.requiredFeatures.add(input.readUTF8());
            case 42 -> this.optionalFeatures.add(input.readUTF8());
            case 130 -> this.writingProgram = input.readUTF8();
            case 138 -> this.source = input.readUTF8();
            case 256 -> this.replicationTimestamp = input.readU64();
            case 264 -> this.replicationSequenceNumber = input.readU64();
            case 274 -> this.replicationBaseUrl = input.readUTF8();

            default -> {
                return false;
            }
        }
        return true;
    }

    public BBox bbox() {
        return bbox;
    }

    public Set<String> requiredFeatures() {
        return requiredFeatures;
    }

    public Set<String> optionalFeatures() {
        return optionalFeatures;
    }

    public String writingProgram() {
        return writingProgram;
    }

    public String source() {
        return source;
    }

    public long replicationTimestamp() {
        return replicationTimestamp;
    }

    public long replicationSequenceNumber() {
        return replicationSequenceNumber;
    }

    public String replicationBaseUrl() {
        return replicationBaseUrl;
    }

    public boolean withHistory() {
        return requiredFeatures.contains(HISTORICAL_INFORMATION);
    }

    @Override
    public String toString() {
        return "Header{" +
               "writingProgram='" + writingProgram + '\'' +
               ", source='" + source + '\'' +
               ", bbox=" + bbox +
               ", requiredFeatures=" + requiredFeatures +
               ", optionalFeatures=" + optionalFeatures +
               ", replicationTimestamp=" + replicationTimestamp +
               ", replicationSequenceNumber=" + replicationSequenceNumber +
               ", replicationBaseUrl='" + replicationBaseUrl + '\'' +
               '}';
    }
}
