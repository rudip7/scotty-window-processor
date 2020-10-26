package Benchmark.Sources;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.XORShiftRandom;

import java.util.Random;

/**
 * generates uniformly distributed IP_V4_Adresses in Integer encoding
 *
 * The adresses are in the first element of the tuple3
 *
 * @author joschavonhein
 */
public class IP_Address_Source extends RichParallelSourceFunction<Tuple3<Integer, Integer, Long>> {

    private final long runtime;
    private final int throughput;

    private Random random = new XORShiftRandom(42);

    public IP_Address_Source(long runtime, int throughput) {

        this.throughput = throughput;
        this.runtime = runtime;
    }

    @Override
    public void run(final SourceFunction.SourceContext<Tuple3<Integer, Integer, Long>> ctx) throws Exception {

        long startTime = System.currentTimeMillis();
        long endTs = startTime + runtime;

        while (System.currentTimeMillis() < endTs){

            long time = System.currentTimeMillis();

            for (int i = 0; i < throughput; i++) {

                ctx.collect(new Tuple3<>(random.nextInt(), 0,time));
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
