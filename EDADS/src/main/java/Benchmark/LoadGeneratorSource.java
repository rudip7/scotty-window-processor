package Benchmark;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.XORShiftRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;


public class LoadGeneratorSource extends RichSourceFunction<Tuple3<Integer, Integer, Long>> {

    private static int maxBackpressure = 5000;
    private final long runtime;

    private static final Logger LOG = LoggerFactory.getLogger(LoadGeneratorSource.class);

    private final int throughput;
    private boolean running = true;

    private final List<Tuple2<Long, Long>> gaps;
    private int currentGapIndex;

    private long nextGapStart = 0;
    private long nextGapEnd;

    private long timeOffset;
    private Random random;

    private Random key;
    private Random value;

    private int median = 10;
    private int standardDeviation = 3;

    public LoadGeneratorSource(long runtime, int throughput, final List<Tuple2<Long, Long>> gaps) {

        this.throughput = throughput;
        this.gaps = gaps;
        this.random = new Random();
        this.runtime = runtime;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.key = new XORShiftRandom(42);
        this.value = new XORShiftRandom(43);
    }

    private int backpressureCounter = 0;

    @Override
    public void run(final SourceContext<Tuple3<Integer, Integer, Long>> ctx) throws Exception {
        long startTime = System.currentTimeMillis();

        ThroughputStatistics.getInstance().pause(false);

        long endTime = startTime + runtime;
        while (running) {
            long startTs = System.currentTimeMillis();

            for (int i = 0; i < throughput; i++) {
                emitValue(readNextTuple(), ctx);
            }

            //Why active waiting?
            while (System.currentTimeMillis() < startTs + 1000) {
                // active waiting
            }

            if(endTime <= System.currentTimeMillis())
                running = false;
        }
    }

    private void emitValue(final Tuple3<Integer, Integer, Long> tuple3, final SourceContext<Tuple3<Integer, Integer, Long>> ctx) {

        if (tuple3.f2 > nextGapStart) {
            ThroughputStatistics.getInstance().pause(true);
            //System.out.println("in Gap");
            if (tuple3.f2 > this.nextGapEnd) {
                ThroughputStatistics.getInstance().pause(false);
                this.currentGapIndex++;
                if (currentGapIndex < gaps.size()) {
                    this.nextGapStart = this.gaps.get(currentGapIndex).f0 + this.timeOffset;
                    this.nextGapEnd = this.nextGapStart + this.gaps.get(currentGapIndex).f1;
                }
            } else
                return;
        }
        ctx.collect(tuple3);
    }

    private Tuple3<Integer, Integer, Long> readNextTuple() throws Exception {
        int newKey = (int) (standardDeviation*key.nextGaussian() + median);
        while (newKey < 0){
            newKey = (int) (standardDeviation*key.nextGaussian() + median);
        }
        return new Tuple3<>(newKey, value.nextInt(10), System.currentTimeMillis());

    }

    @Override
    public void cancel() {
        running = false;
    }
}
