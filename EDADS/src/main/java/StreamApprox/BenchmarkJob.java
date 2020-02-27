package StreamApprox;


import Benchmark.ParallelThroughputLogger;
import Benchmark.Sources.NormalDistributionSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import scala.Int;

import javax.annotation.Nullable;

import java.util.LinkedList;
import java.util.List;

import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

public class BenchmarkJob {

    public static void main(String[] args) throws Exception {

        long runtime;// duration in milli-seconds the source produces data
        int throughput; // desired throughput in tuples / seconds
        int sampleSize; // maximum reservoir size of each stratum
        List<Tuple2<Long, Long>> gaps = new LinkedList<>(); // empty list for source
        int stratification;
        int parallelism;


        if (args.length == 5){
            System.out.println(args[0]);
            System.out.println(args[1]);
            System.out.println(args[2]);
            System.out.println(args[3]);
            System.out.println(args[4]);

            parallelism = Integer.parseInt(args[0]);
            runtime = Long.parseLong(args[1]);
            throughput = Integer.parseInt(args[2]);
            sampleSize = Integer.parseInt(args[3]);
            stratification = Integer.parseInt(args[4]);
        }else {
            throw new Exception("Illegal Number of Arguments!");
        }


        StratifiedReservoirSampling oasrs = new StratifiedReservoirSampling(sampleSize);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple3<Integer, Integer, Long>> messageStream = env.addSource(new NormalDistributionSource(runtime, throughput, gaps));

        messageStream.flatMap(new ParallelThroughputLogger<Tuple3<Integer, Integer, Long>>(1000, "StreamApprox Config - manual"));

        final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
                .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());


        // separate the values into groups
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> mapped = timestamped.map(new MapFunction<Tuple3<Integer, Integer, Long>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Tuple3<Integer, Integer, Long> value) throws Exception {
                int key = value.f0 / 2 < stratification ? value.f0 / 2 : stratification -1;

                return new Tuple2<Integer, Integer>(key, value.f1);
            }
        });


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
