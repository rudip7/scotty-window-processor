package Benchmark.Sources;
import Benchmark.Old.ThroughputStatistics;
import Benchmark.ParallelThroughputLogger;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.zip.GZIPInputStream;

/**
 * This SourceFunction generates a data stream of TaxiRide records which are
 * read from a gzipped input file. Each record has a time stamp and the input file must be
 * ordered by this time stamp.
 * <p>
 * In order to simulate a realistic stream source, the SourceFunction serves events proportional to
 * their timestamps. In addition, the serving of events can be delayed by a bounded random delay
 * which causes the events to be served slightly out-of-order of their timestamps.
 * <p>
 * The serving speed of the SourceFunction can be adjusted by a serving speed factor.
 * A factor of 60.0 increases the logical serving time by a factor of 60, i.e., events of one
 * minute (60 seconds) are served in 1 second.
 * <p>
 * This SourceFunction is an EventSourceFunction and does continuously emit watermarks.
 * Hence it is able to operate in event time mode which is configured as follows:
 * <p>
 * StreamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
 */
public class ZipfDistributionSource extends RichParallelSourceFunction<Tuple3<Integer, Integer, Long>> {

    private final String dataFilePath;

    private transient BufferedReader reader;
    private transient InputStream gzipStream;


    private final long runtime;

    private final int throughput;
    private boolean running = true;

    private final List<Tuple2<Long, Long>> gaps;
    private int currentGapIndex;

    private long nextGapStart = 0;
    private long nextGapEnd;

    private static final Logger LOG = LoggerFactory.getLogger(ParallelThroughputLogger.class);




    /**
     * Serves the TaxiRide records from the specified and ordered gzipped input file.
     * Rides are served exactly in order of their time stamps
     * in a serving speed which is proportional to the specified serving speed factor.
     *
     * @param dataFilePath       The gzipped input file from which the TaxiRide records are read.
     */
    public ZipfDistributionSource(String dataFilePath, long runtime, int throughput) {
        this(dataFilePath, runtime, throughput, new ArrayList<>());
    }

    /**
     * Serves the TaxiRide records from the specified and ordered gzipped input file.
     * Rides are served out-of time stamp order with specified maximum random delay
     * in a serving speed which is proportional to the specified serving speed factor.
     *
     * @param dataFilePath       The gzipped input file from which the TaxiRide records are read.
     *
     */
    public ZipfDistributionSource(String dataFilePath, long runtime, int throughput, final List<Tuple2<Long, Long>> gaps) {
        this.dataFilePath = dataFilePath;
        this.throughput = throughput;
        this.gaps = gaps;
        this.runtime = runtime;
    }

    /**
     * Serves the TaxiRide records from the specified and ordered gzipped input file.
     * Rides are served out-of time stamp order with specified maximum random delay
     * in a serving speed which is proportional to the specified serving speed factor.
     */
    public ZipfDistributionSource(long runtime, int throughput, final List<Tuple2<Long, Long>> gaps) {
        this.dataFilePath = "/share/hadoop/EDADS/zipfTimestamped.gz";
//        this.dataFilePath = "EDADS/Data/zipfTimestamped.gz";
        this.throughput = throughput;
        this.gaps = gaps;
        this.runtime = runtime;
    }

    @Override
    public void run(SourceContext<Tuple3<Integer, Integer, Long>> sourceContext) throws Exception {

        gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
        reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"));


        long startTime = System.currentTimeMillis();
        long endTime = startTime + runtime;
        loop:
        while (running) {
            long startTs = System.currentTimeMillis();

            for (int i = 0; i < throughput; i++) {
                Tuple3<Integer, Integer, Long> tuple = readNextTuple();
                if (tuple == null){
                    LOG.info("Zipf file completely read");
                    System.out.println("Zipf file completely read");
                    break loop;
                }
                sourceContext.collect(tuple);
            }

            if (runtime != -1) {
                while (System.currentTimeMillis() < startTs + 1000) {
                    // active waiting
                }

                if (endTime <= System.currentTimeMillis())
                    running = false;
            }
        }

        this.reader.close();
        this.reader = null;
        this.gzipStream.close();
        this.gzipStream = null;

    }

    private void emitValue(final Tuple3<Integer, Integer, Long> tuple, final SourceContext<Tuple3<Integer, Integer, Long>> ctx) {

        if (tuple.f2 > nextGapStart) {
            ThroughputStatistics.getInstance().pause(true);
            //System.out.println("in Gap");
            if (tuple.f2 > this.nextGapEnd) {
                ThroughputStatistics.getInstance().pause(false);
                this.currentGapIndex++;
                if (currentGapIndex < gaps.size()) {
                    this.nextGapStart = this.gaps.get(currentGapIndex).f0;
                    this.nextGapEnd = this.nextGapStart + this.gaps.get(currentGapIndex).f1;
                }
            } else
                return;
        }
        ctx.collect(tuple);
    }

    private Tuple3<Integer, Integer, Long> readNextTuple() throws Exception {
        String line;
        if (reader.ready() && (line = reader.readLine()) != null) {
            // read first tuple
            return fromString(line);
        } else {
            return null;
        }
    }

    /**
     * f0:  key            : Integer      // a random integer selected from a ZIPF distribution
     * f1:  value          : Integer      // a random integer
     * f2:  eventTime      : Long
     */
    public Tuple3<Integer, Integer, Long> fromString(String line) {

        String[] tokens = line.split(",");
        if (tokens.length != 3) {
            throw new RuntimeException("Invalid record: " + line);
        }

        Tuple3<Integer, Integer, Long> tuple = new Tuple3<>();

        try {
            tuple.f0 = Integer.parseInt(tokens[0]);
//            tuple.f1 = Integer.parseInt(tokens[1]);
            tuple.f1 = this.getRuntimeContext().getIndexOfThisSubtask();
//            tuple.f2 = Long.parseLong(tokens[2]);
            if (runtime == -1){
                tuple.f2 = 1603388370564L;
            } else {
                tuple.f2 = System.currentTimeMillis();
            }

        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }

        return tuple;
    }

    @Override
    public void cancel() {
        try {
            if (this.reader != null) {
                this.reader.close();
            }
            if (this.gzipStream != null) {
                this.gzipStream.close();
            }
        } catch (IOException ioe) {
            throw new RuntimeException("Could not cancel SourceFunction", ioe);
        } finally {
            this.reader = null;
            this.gzipStream = null;
            this.running = false;
        }
    }

}
