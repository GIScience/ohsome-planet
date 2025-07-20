package org.heigit.ohsome.contributions.rocksdb;

import org.rocksdb.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Consumer;

import static org.rocksdb.CompactionPriority.MinOverlappingRatio;
import static org.rocksdb.CompressionType.LZ4_COMPRESSION;
import static org.rocksdb.CompressionType.ZSTD_COMPRESSION;
import static org.rocksdb.util.SizeUnit.KB;
import static org.rocksdb.util.SizeUnit.MB;

public class RocksUtil {

    private RocksUtil() {
        // utility class
    }

    public static Options defaultOptions() {
        return defaultOptions((Cache)null);
    }
    public static Options defaultOptions(Cache blockCache) {
        var options = new Options();
        defaultOptions(options, blockCache);
        return options;
    }

    public static void defaultOptions(DBOptions options) {
        defaultMDBOptions(options);
    }

    public static BlockBasedTableConfig defaultOptions(Options options, Cache blockCache) {
        defaultMDBOptions(options);

        defaultCFOptions(options);
        defaultMCFOptions(options);

        final var tableOptions = new BlockBasedTableConfig();
        options.setTableFormatConfig(tableOptions);
        defaultTableConfig(tableOptions, blockCache);

        return tableOptions;
    }

    public static BlockBasedTableConfig defaultOptions(ColumnFamilyOptions options, Cache blockCache) {
        defaultCFOptions(options);
        defaultMCFOptions(options);
        final var tableOptions = new BlockBasedTableConfig();
        options.setTableFormatConfig(tableOptions);
        defaultTableConfig(tableOptions, blockCache);
        return tableOptions;
    }

    public static void defaultMDBOptions(MutableDBOptionsInterface<?> options) {
        options.setMaxBackgroundJobs(6);
        options.setBytesPerSync(1L * MB);
    }

    public static void defaultCFOptions(ColumnFamilyOptionsInterface<?> options) {
        options.setBottommostCompressionType(ZSTD_COMPRESSION);
        // general options
        options.setLevelCompactionDynamicLevelBytes(true);
        options.setCompactionPriority(MinOverlappingRatio);
    }

    public static void defaultMCFOptions(MutableColumnFamilyOptionsInterface<?> options) {
        options.setCompressionType(LZ4_COMPRESSION);
    }

    public static void defaultTableConfig(BlockBasedTableConfig tableOptions, Cache blockCache) {
        tableOptions.setBlockSize(16 * KB);
        tableOptions.setCacheIndexAndFilterBlocks(true);
        tableOptions.setBlockCache(blockCache);

        // new format 4 options
        tableOptions.setFormatVersion(4);
        tableOptions.setIndexBlockRestartInterval(16);
    }

    public static ColumnFamilyHandle createColumnFamily(RocksDB db, Cache blockCache, String name,
                                                        Consumer<ColumnFamilyOptions> modify) throws RocksDBException {
        var cfOpts = new ColumnFamilyOptions();
        var tableOptions = RocksUtil.defaultOptions(cfOpts, blockCache);
        if (blockCache != null) {
            tableOptions.setBlockCache(blockCache);
        }
        modify.accept(cfOpts);
        return db.createColumnFamily(new ColumnFamilyDescriptor(name.getBytes(), cfOpts));
    }

    public static void destroy(String path) throws RocksDBException {
        RocksDB.destroyDB(path, new Options());
    }

    public static WriteOptions disableWAL() {
        WriteOptions writeOptions = new WriteOptions();
        writeOptions.setDisableWAL(true);
        return writeOptions;
    }

    public static long longFromByteArray(byte[] a) {
        return ByteBuffer.wrap(a).order(ByteOrder.LITTLE_ENDIAN).getLong(0);
    }

    public static byte[] longToByteArray(long l) {
        return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN).putLong(0, l).array();

    }

}
