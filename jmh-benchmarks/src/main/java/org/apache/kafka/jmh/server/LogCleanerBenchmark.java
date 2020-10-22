package org.apache.kafka.jmh.server;

import kafka.api.ApiVersion;
import kafka.log.AppendOrigin;
import kafka.log.CleanedTransactionMetadata;
import kafka.log.Cleaner;
import kafka.log.CleanerStats;
import kafka.log.Log;
import kafka.log.LogCleaner$;
import kafka.log.LogConfig;
import kafka.log.LogUtils;
import kafka.log.MurmurOffsetMap;
import kafka.log.SkimpyOffsetMap;
import kafka.utils.MockScheduler;
import kafka.utils.Scheduler;
import kafka.utils.TestUtils;
import kafka.utils.Throttler;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import scala.collection.immutable.Set$;
import scala.runtime.BoxedUnit;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 2)
@Measurement(iterations = 3)
@Fork(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(value = Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
public class LogCleanerBenchmark {

    @Param({"10485760"})
    public int memory;

    @Param({"65536"})
    public int maxMessageSize;

    //"1024", "10485760", "104857600", "524288000", "1073741824"
    @Param({"10485760", "104857600", "524288000", "1073741824"})
    public int segmentSize;

    @Param({"10", "100"})
    public int numKeys;

    @Param({"10"})
    public int keyLength;

    private Random random = new Random();

    private SkimpyOffsetMap skimpyOffsetMap;
    private MurmurOffsetMap murmurOffsetMap;
    private Cleaner md5Cleaner;
    private Cleaner murCleaner;
    private CleanerStats md5CleanerStats;
    private CleanerStats murCleanerStats;
    private Log log;
    private Time time;
    private Scheduler scheduler;
    private File dir;
    private String[] keys;

    @Setup(Level.Invocation)
    public void setupTrial() {
        time = Time.SYSTEM;
        scheduler = new MockScheduler(time);
        skimpyOffsetMap = new SkimpyOffsetMap(memory, "MD5");
        murmurOffsetMap = new MurmurOffsetMap(memory);
        Throttler throttler = new Throttler(Double.MAX_VALUE, Long.MAX_VALUE,
            true, "throttler", "entries", time);
        md5Cleaner = new Cleaner(0, skimpyOffsetMap, maxMessageSize, maxMessageSize,
            0.75, throttler, time, tp -> BoxedUnit.UNIT);
        murCleaner = new Cleaner(0, murmurOffsetMap, maxMessageSize, maxMessageSize,
            0.75, throttler, time, tp -> BoxedUnit.UNIT);
        md5CleanerStats = new CleanerStats(time);
        murCleanerStats = new CleanerStats(time);
        Properties logProps = new Properties();
        logProps.put(LogConfig.SegmentBytesProp(), segmentSize);
        logProps.put(LogConfig.CleanupPolicyProp(), LogConfig.Compact());
        logProps.put(LogConfig.MessageTimestampDifferenceMaxMsProp(), Long.toString(Long.MAX_VALUE));
        LogConfig config = new LogConfig(logProps, Set$.MODULE$.empty());

        dir = TestUtils.randomPartitionLogDir(TestUtils.tempDir());

        log = LogUtils.createLog(dir, config, time, scheduler, 0);

        Set<String> keysSet = new HashSet<>(numKeys);
        for (int i = 0; i < numKeys; ++i) {
            String nextRandom;
            do {
                nextRandom = nextRandomString(keyLength);
            } while (keysSet.contains(nextRandom));
            keysSet.add(nextRandom);
        }
        keys = new ArrayList<>(keysSet).toArray(new String[]{});

        long value = 0;
        while (log.numberOfSegments() < 4) {
            log.appendAsLeader(
                TestUtils.singletonRecords(
                    String.valueOf(value).getBytes(),
                    keys[random.nextInt(numKeys)].getBytes(),
                    CompressionType.NONE,
                    RecordBatch.NO_TIMESTAMP,
                    RecordBatch.CURRENT_MAGIC_VALUE
                    ), 0, AppendOrigin.Client$.MODULE$, ApiVersion.latestVersion());
            value++;
        }
    }

    private String nextRandomString(int length) {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 10;
        return random.ints(leftLimit, rightLimit + 1)
            .limit(targetStringLength)
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString();
    }

    @Benchmark
    @Threads(1)
    public void logCleanerWithMd5OffsetMap() {
        md5Cleaner.buildOffsetMap(log, log.logStartOffset(), log.logEndOffset(), skimpyOffsetMap, md5CleanerStats);
//        md5Cleaner.buildOffsetMap(log, log.logStartOffset(), log.logEndOffset(), skimpyOffsetMap, new CleanerStats(time));
//        CleanerStats stats = new CleanerStats(time);
        md5Cleaner.cleanSegments(log, log.logSegments().toSeq(), skimpyOffsetMap, 0L, md5CleanerStats, new CleanedTransactionMetadata());
    }

    @Benchmark
    @Threads(1)
    public void logCleanerWithMurmurOffsetMap() {
        murCleaner.buildOffsetMap(log, log.logStartOffset(), log.logEndOffset(), murmurOffsetMap, murCleanerStats);
//        CleanerStats stats = new CleanerStats(time);
        murCleaner.cleanSegments(log, log.logSegments().toSeq(), murmurOffsetMap, 0L, murCleanerStats, new CleanedTransactionMetadata());
    }

    @TearDown(Level.Invocation)
    public void tearDown() {
        printStats(0, "MD5 Map", log.logStartOffset(), log.logEndOffset(), md5CleanerStats);
        printStats(0, "Mur Map", log.logStartOffset(), log.logEndOffset(), murCleanerStats);
    }

    private double mb(long bytes) {
        return (double) bytes / (1024*1024);
    }

    private double mb(double bytes) {
        return bytes / (1024*1024);
    }

    private void printStats(int id, String name, long from, long to,  CleanerStats stats) {
        String message =
            String.format("%n\tLog cleaner thread %d cleaned log %s (dirty section = [%d, %d])%n", id, name, from, to) +
                String.format("\t%,.1f MB of log processed in %,.1f seconds (%,.1f MB/sec).%n", mb(stats.bytesRead()),
                    stats.elapsedSecs(),
                    mb(stats.bytesRead() / stats.elapsedSecs())) +
                String.format("\tIndexed %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n", mb(stats.mapBytesRead()),
                    stats.elapsedIndexSecs(),
                    mb(stats.mapBytesRead()) / stats.elapsedIndexSecs(),
                    100 * stats.elapsedIndexSecs() / stats.elapsedSecs()) +
                String.format("\tBuffer utilization: %.1f%%%n", 100 * stats.bufferUtilization()) +
                String.format("\tCleaned %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n", mb(stats.bytesRead()),
                    stats.elapsedSecs() - stats.elapsedIndexSecs(),
                    mb(stats.bytesRead()) / (stats.elapsedSecs() - stats.elapsedIndexSecs()), 100 * (stats.elapsedSecs() - stats.elapsedIndexSecs()) / stats.elapsedSecs()) +
                String.format("\tStart size: %,.1f MB (%,d messages)%n", mb(stats.bytesRead()), stats.messagesRead()) +
                String.format("\tEnd size: %,.1f MB (%,d messages)%n", mb(stats.bytesWritten()), stats.messagesWritten()) +
                String.format("\t%.1f%% size reduction (%.1f%% fewer messages)%n", 100.0 * (1.0 - (double)stats.bytesWritten()/stats.bytesRead()),
                    100.0 * (1.0 - (double)stats.messagesWritten()/stats.messagesRead()));
        System.out.println(message);
    }
}
