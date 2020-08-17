package Benchmark.Sources;

import ApproximateDataAnalytics.TimestampedQuery;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;

public class TimestampedQuerySource implements SourceFunction<TimestampedQuery<Double>> {

    private final long runtime;
    private final int throughput;
    private final long wait;

    public TimestampedQuerySource(Time runtime, Time wait, int throughput) {
        this.wait = wait.toMilliseconds();
        this.runtime = runtime.toMilliseconds() - this.wait;
        this.throughput = throughput;
    }

    @Override
    public void run(SourceContext<TimestampedQuery<Double>> ctx) throws Exception {
        Random random = new Random();

        long startTs = System.currentTimeMillis();
        long endTs = startTs + wait + runtime;

        while (System.currentTimeMillis() < startTs + wait) {
            // active waiting
        }

        while (System.currentTimeMillis() < endTs){

            long time = System.currentTimeMillis();
            int upperBound = (int)((time - startTs) / 1000);

            for (int i = 0; i < throughput; i++) {
                int relativeTimestamp = random.nextInt(upperBound); // upper bound in seconds from job start
                long query_timestamp = startTs + relativeTimestamp * 1000;
                ctx.collectWithTimestamp(new TimestampedQuery<Double>(random.nextDouble(), query_timestamp), time);
            }

            while (System.currentTimeMillis() < time + 1000) {
                // active waiting
            }
        }
    }

    @Override
    public void cancel() {

    }
}