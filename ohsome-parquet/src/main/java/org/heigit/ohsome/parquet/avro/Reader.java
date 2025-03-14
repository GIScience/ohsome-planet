package org.heigit.ohsome.parquet.avro;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;

import java.io.IOException;
import java.nio.file.Path;

public class Reader {

    public static void main(String[] args) throws IOException {
        var inputFile = new LocalInputFile(Path.of("out/relation-005.parquet"));

        // get metadata about row groups!
        try (var reader = ParquetFileReader.open(inputFile)) {
            var footer = reader.getFooter();
            var fileMetaData = footer.getFileMetaData();
            var blocks = footer.getBlocks();
            var blockId = 0;
            for (var block : blocks) {
                System.out.println("blockId = " + blockId++);
                var columns = block.getColumns();
                for(var column : columns) {
                    System.out.print(" " + column.getPath().toDotString());
                    System.out.print(", " + column.getStatistics());
                    System.out.println(", " + column.hasDictionaryPage());
                }
                break;
            }
        }
    }
}
