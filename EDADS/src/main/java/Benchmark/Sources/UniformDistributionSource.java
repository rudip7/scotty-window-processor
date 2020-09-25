package Benchmark.Sources;

import Benchmark.Old.ThroughputStatistics;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.XORShiftRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;


public class UniformDistributionSource extends RichParallelSourceFunction<Tuple3<Integer, Integer, Long>> {

    private static int maxBackpressure = 5000;
    private final long runtime;

    private static final Logger LOG = LoggerFactory.getLogger(NormalDistributionSource.class);

    private final int throughput;
    private boolean running = true;

    private final List<Tuple2<Long, Long>> gaps;
    private int currentGapIndex;

    private long nextGapStart = 0;
    private long nextGapEnd;

    private long timeOffset;
    private Random random;

    private Random key;

    private int median = 10;
    private int standardDeviation = 3;


    /**
     * Construct new UniformDistributionSource
     * @param runtime
     * @param throughput
     * @param gaps
     */
    public UniformDistributionSource(long runtime, int throughput, final List<Tuple2<Long, Long>> gaps) {

        this.throughput = throughput;
        this.gaps = gaps;
        this.random = new XORShiftRandom();
        this.runtime = runtime;
    }

    @Override
    /**
     * set initialization parameters to generate uniformly distributed stream elements
     * @param parameters
     * @throws Exception
     */
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.key = new XORShiftRandom(42);
    }

    private int backpressureCounter = 0;

    @Override
    /**
     * emit generated elements until running time does not exceed specified runtime
     *
     * @param ctx
     */
    public void run(final SourceContext<Tuple3<Integer, Integer, Long>> ctx) throws Exception {
        long startTime = System.currentTimeMillis();

//        ThroughputStatistics.getInstance().pause(false);

        long endTime = startTime + runtime;
        while (running) {
            long startTs = System.currentTimeMillis();

            for (int i = 0; i < throughput; i++) {
                emitValue(readNextTuple(), ctx);
            }

            while (System.currentTimeMillis() < startTs + 1000) {
                // active waiting
            }

            if(endTime <= System.currentTimeMillis())
                running = false;
        }
    }

    /**
     * emit input value, if there should be a gap based on values in gaps list apply it.
     *
     * @param tuple3 input value
     * @param ctx
     */
    private void emitValue(final Tuple3<Integer, Integer, Long> tuple3, final SourceContext<Tuple3<Integer, Integer, Long>> ctx) {

        if (tuple3.f2 > nextGapStart) {
            ThroughputStatistics.getInstance().pause(true);
            //Environment.out.println("in Gap");
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

    /**
     * generate a timestamped tuple of calculated values
     *
     * @return generated tuple
     * @throws Exception
     */
    private Tuple3<Integer, Integer, Long> readNextTuple() throws Exception {
        int newKey = key.nextInt(101);
//        while (newKey < 0){
//            newKey = (int) (standardDeviation*key.nextGaussian() + median);
//        }
        return new Tuple3<>(newKey, key.nextInt(10), System.currentTimeMillis());

    }

    /**
     * cancel generating data stream
     */
    @Override
    public void cancel() {
        running = false;
    }
}
