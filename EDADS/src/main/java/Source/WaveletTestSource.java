package Source;

import Benchmark.Old.ThroughputStatistics;
import FlinkScottyConnector.BuildSynopsis;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.XORShiftRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;


public class WaveletTestSource extends RichParallelSourceFunction<Tuple3<Integer, Integer, Long>> {

    private final long runtime;

    private final int throughput;
    private boolean running = true;
    private int currentGapIndex;

    private long nextGapStart = 0;
    private long nextGapEnd;

    private long timeOffset;
    private Random random;

    private Random key;
    private BuildSynopsis.IntegerState value;

    private int median = 10;
    private int standardDeviation = 3;

    public WaveletTestSource(long runtime, int throughput) {

        this.throughput = throughput;
        this.random = new Random();
        this.runtime = runtime;
        this.value = new BuildSynopsis.IntegerState();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.key = new XORShiftRandom(42);
    }

    private int backpressureCounter = 0;

    @Override
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

    private void emitValue(final Tuple3<Integer, Integer, Long> tuple3, final SourceContext<Tuple3<Integer, Integer, Long>> ctx) {
        ctx.collect(tuple3);
    }

    private Tuple3<Integer, Integer, Long> readNextTuple() throws Exception {
        int newKey = (int) (standardDeviation*key.nextGaussian() + median);
        while (newKey < 0){
            newKey = (int) (standardDeviation*key.nextGaussian() + median);
        }
//        newKey = 7;
        int v = value.value();
        value.update(v+1);
        return new Tuple3<>(newKey, v, System.currentTimeMillis());
    }

    @Override
    public void cancel() {
        running = false;
    }
}
