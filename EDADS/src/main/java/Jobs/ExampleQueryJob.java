package Jobs;


import Benchmark.Sources.ZipfDistributionSource;
import FlinkScottyConnector.BuildSynopsis;
import FlinkScottyConnector.SynopsisAggregator;
import Synopsis.Sketches.DDSketch;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import javax.swing.plaf.TabbedPaneUI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

public class ExampleQueryJob {
    public static void main(String[] args) {
        // Arguments
        final int runtime = 20000; // runtime in milliseconds
        final int throughput = 100; // target throughput
        final List<Tuple2<Long, Long>> gaps = new ArrayList<>();
        final double accuracy = 0.95; // relative accuracy of DD-Sketch
        final int maxNumberOfBins = 100; // maximum number of bins of DD-Sketch
        final String pathToZipfData = "/Users/joschavonhein/Data/zipfTimestamped.gz";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Tuple3<Integer, Integer, Long>> messageStream = env.addSource(new ZipfDistributionSource(pathToZipfData, runtime, throughput, gaps));

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

        final SingleOutputStreamOperator<DDSketch> partialAggregate = windowedStream
                .aggregate(agg).returns(DDSketch.class);

        final SingleOutputStreamOperator<Tuple3<DDSketch, Long, Long>> timestampedSketches = partialAggregate.timeWindowAll(seconds(6L), seconds(3L))
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

        KeyedStream<Tuple4<DDSketch, Long, Long, Long>, Tuple> sketchWithKey = timestampedSketches.map(new MapFunction<Tuple3<DDSketch, Long, Long>, Tuple4<DDSketch, Long, Long, Long>>() {
            @Override
            public Tuple4<DDSketch, Long, Long, Long> map(Tuple3<DDSketch, Long, Long> value) throws Exception {
                return new Tuple4<>(value.f0, value.f1, value.f2, value.f1 / 60000);
            }
        }).keyBy(3);

        timestampedSketches.writeAsText("/Users/joschavonhein/Workspace/scotty-window-processor/EDADS/Results/timestampedSketches.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // Query Input Stream

        final KeyedStream<Tuple3<Long, Double, Long>, Tuple> querySource = env.fromElements(new Tuple2<>(1517499753000L, 0.5), new Tuple2<>(1517499759000L, 0.1))
                .assignTimestampsAndWatermarks(new QueryTimeStampAssigner())
                .map(new MapFunction<Tuple2<Long, Double>, Tuple3<Long, Double, Long>>() {
                    @Override
                    public Tuple3<Long, Double, Long> map(Tuple2<Long, Double> value) throws Exception {
                        return new Tuple3<>(value.f0, value.f1, value.f0 / 60000);
                    }
                }).keyBy(2);

        // querySource.writeAsText("/Users/joschavonhein/Workspace/scotty-window-processor/EDADS/Results/queries.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);


        // Join QueryStream with Sketch Stream using an intervalJoin with a Process Function
        final SingleOutputStreamOperator<Tuple2<Double, String>> queryResultStream = querySource
                .intervalJoin(sketchWithKey)
                .between(Time.days(-1), Time.days(1)) // this join is based on event timestamps -> the query can be joined with any sketch built in the last 24 hour -> the actual join is then based on the key (=> query Timestamp and sketch Timestamp)
                .process(new ProcessJoinFunction<Tuple3<Long, Double, Long>, Tuple4<DDSketch, Long, Long, Long>, Tuple2<Double, String>>() {
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss:sss");

                    @Override
                    public void processElement(Tuple3<Long, Double, Long> left, Tuple4<DDSketch, Long, Long, Long> right, Context ctx, Collector<Tuple2<Double, String>> out) throws Exception {
                        if (left.f0 >= right.f1 && left.f0 <= right.f2) { // the query time is contained within the sketch window time

                            double valueAtQuantile = right.f0.getValueAtQuantile(left.f1);
                            String queryResult = "Query for Timestamp: " + formatter.format(new Date(left.f0)) + " \n" +
                                    "value at Quantile " + left.f1 + " = " + valueAtQuantile + "\n" +
                                    "window start time: " + formatter.format(new Date(right.f1)) + "  --  window end time: " + formatter.format(new Date(right.f2));
                            out.collect(new Tuple2<>(valueAtQuantile, queryResult));
                        }
                    }
                });

        queryResultStream.writeAsText("/Users/joschavonhein/Workspace/scotty-window-processor/EDADS/Results/queryResults.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);



        try {
            env.execute("Use Case Example Job with DD-Sketch");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class TimestampsAndWatermarks implements AssignerWithPeriodicWatermarks<Tuple3<Integer, Integer, Long>> {
        private long currentMaxTimestamp;

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

    public static class QueryTimeStampAssigner implements AssignerWithPeriodicWatermarks<Tuple2<Long, Double>> {
        private long currentMaxTimestamp;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp);
        }

        @Override
        public long extractTimestamp(Tuple2<Long, Double> element, long previousElementTimestamp) {
            long timestamp = element.f0;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }
}
