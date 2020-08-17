package Benchmark.Sources;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;

public class StratifiedQuerySource implements SourceFunction<Tuple2<Integer, Double>> {

    private final long runtime;
    private final int throughput;
    private final long wait;
    private final int stratification;


    public StratifiedQuerySource(Time runtime, int throughput, Time wait, int stratification) {
        this.wait = wait.toMilliseconds();
        this.runtime = runtime.toMilliseconds();
        this.throughput = throughput;
        this.stratification = stratification;
    }

    @Override
    public void run(SourceContext<Tuple2<Integer, Double>> ctx) throws Exception {
        Random random = new Random();

        long startTs = System.currentTimeMillis();
        long endTs = startTs + runtime;

        while (System.currentTimeMillis() < startTs + wait) {
            // active waiting
        }

        while (System.currentTimeMillis() < endTs){

            long time = System.currentTimeMillis();

            for (int i = 0; i < throughput; i++) {
                ctx.collectWithTimestamp(new Tuple2<>(random.nextInt(stratification), random.nextDouble()), System.currentTimeMillis());
            }

            while (System.currentTimeMillis() < time + 1000) {
                // active waiting
            }
        }
    }

    @Override
    public void cancel() { }
}
