package Jobs;


import Benchmark.FlinkBenchmarkJobs.ZipfFlinkJob;
import Benchmark.ParallelThroughputLogger;
import Benchmark.Sources.ZipfDistributionSource;
import FlinkScottyConnector.BuildSynopsis;
import FlinkScottyConnector.SynopsisAggregator;
import Synopsis.MergeableSynopsis;
import Synopsis.Sampling.SamplerWithTimestamps;
import Synopsis.Sketches.DDSketch;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

public class ExampleQueryJob {
    public static void main(String[] args) {
        // Arguments
        final int runtime = 20000; // runtime in milliseconds
        final int throughput = 100000; // target throughput
        final List<Tuple2<Long, Long>> gaps = new ArrayList<>();
        final double accuracy = 0.95; // relative accuracy of DD-Sketch
        final int maxNumberOfBins = 100; // maximum number of bins of DD-Sketch

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Tuple3<Integer, Integer, Long>> messageStream = env.addSource(new ZipfDistributionSource(runtime, throughput, gaps));

        final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
                .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());

        // build the DD-Sketch on the first element of the Tuple3
        // SingleOutputStreamOperator<DDSketch> ddSketches = BuildSynopsis.timeBased(messageStream, seconds(6L), seconds(3L),0, DDSketch.class, accuracy, maxNumberOfBins);

        // this logic is supposed to be in BuildSynopsis.timeBased TODO: after succesful testing move there
        Object[] params = new Object[]{accuracy, maxNumberOfBins};
        SynopsisAggregator agg = new SynopsisAggregator(DDSketch.class, params);

        final KeyedStream keyBy = timestamped.rescale()
                .map(new BuildSynopsis.AddParallelismIndex(0))
                .keyBy(0);

        final WindowedStream windowedStream = keyBy.timeWindow(seconds(6L), seconds(3L));

        SingleOutputStreamOperator partialAggregate = windowedStream
                .aggregate(agg);

        SingleOutputStreamOperator<Tuple3<DDSketch, Long, Long>> timestampedSketches = partialAggregate.timeWindowAll(seconds(6L), seconds(3L))
                .reduce(new ReduceFunction<DDSketch>() {
                    public DDSketch reduce(DDSketch value1, DDSketch value2) throws Exception {
                        DDSketch merged = value1.merge(value2);
                        return merged;
                    }
                }, new AllWindowFunction<DDSketch, Tuple3<DDSketch, Long, Long>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable values, Collector out) throws Exception {
                        values.forEach(new Consumer<DDSketch>() {
                            @Override
                            public void accept(DDSketch sketch) {
                                out.collect(new Tuple3<>(sketch, window.getStart(), window.getEnd()));
                            }
                        });
                    }
                }); // if there is an error here: try .returns!




        DataStreamSource<Tuple2<Long, Double>> querySource = env.fromElements("");


        queryAndLong.keyBy(0)
                .intervalJoin(timestampedSketches.keyBy(1))
                .between(Time.days(-1), seconds(0)) // this join is based on event timestamps -> the query can be joined with any sketch built in the last 24 hour -> the actual join is then based on the key (=> query Timestamp and sketch Timestamp)
                .process(new ProcessJoinFunction<Tuple2<Long, String>, Tuple3<DDSketch, Long, Long>, Double>() {
                    @Override
                    public void processElement(Tuple2<Long, String> left, Tuple3<DDSketch, Long, Long> right, Context ctx, Collector<Double> out) throws Exception {
                        right.f0.getValueAtQuantile()
                    }
                });
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
