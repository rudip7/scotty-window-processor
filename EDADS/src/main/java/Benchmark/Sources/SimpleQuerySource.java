package Benchmark.Sources;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;

public class SimpleQuerySource extends RichParallelSourceFunction<Double> {

    private final long runtime;
    private final int throughput;
    private final long wait;

    public SimpleQuerySource(Time runtime, int throughput, Time wait) {
        this.runtime = runtime.toMilliseconds();
        this.throughput = throughput;
        this.wait = wait.toMilliseconds();
    }

    @Override
    public void run(SourceContext<Double> ctx) throws Exception {
        Random random = new Random();

        long startTs = System.currentTimeMillis();
        long endTs = startTs + runtime;

        while (System.currentTimeMillis() < startTs + wait) {
            // active waiting
        }

        while (System.currentTimeMillis() < endTs){

            long time = System.currentTimeMillis();

            for (int i = 0; i < throughput; i++) {
                ctx.collectWithTimestamp(random.nextDouble(), System.currentTimeMillis());
            }

            while (System.currentTimeMillis() < time + 1000) {
                // active waiting
            }
        }
    }

    @Override
    public void cancel() { }
}
