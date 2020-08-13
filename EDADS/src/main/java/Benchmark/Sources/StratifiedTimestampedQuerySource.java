package Benchmark.Sources;

import ApproximateDataAnalytics.TimestampedQuery;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;

public class StratifiedTimestampedQuerySource implements SourceFunction<Tuple2<Integer, TimestampedQuery<Double>>> {

    private final int throughput;
    private final long wait;
    private final long runtime;
    private final int stratification;


    public StratifiedTimestampedQuerySource(int throughput, Time wait, Time runtime, int stratification) {
        this.throughput = throughput;
        this.wait = wait.toMilliseconds();
        this.runtime = runtime.toMilliseconds() - this.wait;
        this.stratification = stratification;
    }

    @Override
    public void run(SourceContext<Tuple2<Integer, TimestampedQuery<Double>>> ctx) throws Exception {
        Random random = new Random();

        long startTs = System.currentTimeMillis();
        long endTs = startTs + wait + runtime;

        while (System.currentTimeMillis() < startTs + wait) {
            // active waiting
        }

        // collect TimestampedQueries with target maximum Throughput
        while (System.currentTimeMillis() < endTs){

            long time = System.currentTimeMillis();
            int upperBound = (int)((time - startTs) / 1000);

            for (int i = 0; i < throughput; i++) {
                int relativeTimestamp = random.nextInt(upperBound); // upper bound in seconds from job start
                long query_timestamp = startTs + relativeTimestamp * 1000;
                TimestampedQuery<Double> timestampedQuery = new TimestampedQuery<Double>(random.nextDouble(), query_timestamp);
                ctx.collectWithTimestamp(new Tuple2<>(random.nextInt(stratification), timestampedQuery), time);
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