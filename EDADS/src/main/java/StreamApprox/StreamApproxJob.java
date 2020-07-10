package StreamApprox;


import Benchmark.ParallelThroughputLogger;
import Benchmark.Sources.NormalDistributionSource;
import Benchmark.Sources.UniformDistributionSource;
import Benchmark.Sources.ZipfDistributionSource;
import de.tub.dima.scotty.state.ValueState;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

public class StreamApproxJob {

    public StreamApproxJob(ApproxConfiguration config, StreamExecutionEnvironment env, String configString){

        StratifiedReservoirSampling oasrs = new StratifiedReservoirSampling(config.sampleSize);
        List<Tuple2<Long, Long>> gaps = Collections.emptyList();

        DataStreamSource<Tuple3<Integer, Integer, Long>> messageStream = null;
        if (config.source == Source.Zipf){
            messageStream = env.addSource(new ZipfDistributionSource(config.runtime, config.throughput, gaps));
        }else if (config.source == Source.Uniform){
            messageStream = env.addSource(new UniformDistributionSource(config.runtime, config.throughput, gaps));
        }

        messageStream.flatMap(new ParallelThroughputLogger<Tuple3<Integer, Integer, Long>>(1000, configString));

        final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
                .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());


        // separate the values into groups
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> mapped = timestamped.map(new RichStratifier());


        // build reservoir sample
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Double>> sampled = mapped.transform("sampling", TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, Double>>() {
        }), new Sampler<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>>(oasrs));


        sampled.addSink(new SinkFunction<Tuple3<Integer, Integer, Double>>() {
            @Override
            public void invoke(Tuple3<Integer, Integer, Double> value, Context context) throws Exception {

            }
        });


        try {
            env.execute("StreamApprox-Job");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static class TimestampsAndWatermarks implements AssignerWithPeriodicWatermarks<Tuple3<Integer, Integer, Long>> {
        private final long maxOutOfOrderness = seconds(20).toMilliseconds(); // 5 seconds
        private long currentMaxTimestamp;
        private long startTime = System.currentTimeMillis();

        @Override
        public long extractTimestamp(final Tuple3<Integer, Integer, Long> element, final long previousElementTimestamp) {
            long timestamp = element.f2;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp);
        }

    }
}
