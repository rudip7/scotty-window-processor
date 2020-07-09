package Jobs;


import Benchmark.Sources.UniformDistributionSource;
import Benchmark.Sources.ZipfDistributionSource;
import FlinkScottyConnector.BuildSynopsisConfig;
import FlinkScottyConnector.NewBuildSynopsis;
import Synopsis.Sketches.DDSketch;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.*;

public class ExampleQueryJob {
    public static void main(String[] args) {

        // Arguments & setup
        final int runtime = 20000; // runtime in milliseconds
        final int throughput = 100; // target throughput
        final List<Tuple2<Long, Long>> gaps = new ArrayList<>();
        final double accuracy = 0.95; // relative accuracy of DD-Sketch
        final int maxNumberOfBins = 100; // maximum number of bins of DD-Sketch
        final String pathToZipfData = "/Users/joschavonhein/Data/zipfTimestamped.gz";
        Object[] params = new Object[]{accuracy, maxNumberOfBins};
        BuildSynopsisConfig config = new BuildSynopsisConfig(Time.seconds(6L), Time.seconds(3L), 0);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // DataStreamSource<Tuple3<Integer, Integer, Long>> messageStream = env.addSource(new ZipfDistributionSource(pathToZipfData, runtime, throughput, gaps));
        DataStreamSource<Tuple3<Integer, Integer, Long>> messageStream = env.addSource(new UniformDistributionSource(runtime, throughput, gaps));

        final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
                .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());


        // Run Job with Interval Join
        // runWithIntervalJoin(timestamped, env, config, params);

        // Run Job with DualInputOperator
        runWithDualInputOperator(timestamped, env, config, params);

    }

    private static void runWithDualInputOperator(SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped, StreamExecutionEnvironment env, BuildSynopsisConfig config, Object... params){

        /*
        SingleOutputStreamOperator<DDSketch> sketches = NewBuildSynopsis.timeBased(timestamped, DDSketch.class, config, params)
                .returns(DDSketch.class);
        */
        SingleOutputStreamOperator<Tuple3<DDSketch, Long, Long>> timestampedSketches = NewBuildSynopsis.timeBasedWithWindowTimes(timestamped, DDSketch.class, config, params)
                .returns(new TypeHint<Tuple3<DDSketch, Long, Long>>() {
                });



        SingleOutputStreamOperator<Double> quantiles = env.addSource(new QuerySource(20));

        /*
        SingleOutputStreamOperator<Tuple2<Double, Double>> queryResults = sketches.connect(quantiles)
                .process(new CustomCoProcessFunction()); //.setParallelism(1);

         */

        SingleOutputStreamOperator<Tuple3<Double, Double, Long>> queryResults = timestampedSketches.connect(quantiles)
                .process(new CustomTimeStampCoProcessFunction()).setParallelism(1);

        queryResults.writeAsText("/Users/joschavonhein/Workspace/scotty-window-processor/EDADS/Results/RichCoProcessResult.txt", FileSystem.WriteMode.OVERWRITE); //.setParallelism(1);

        try {
            env.execute("Query using CoProcessFunction");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class QuerySource implements SourceFunction<Double> {

        private final int queries;

        public QuerySource() {
            queries = 10;
        }

        public QuerySource(int queriesCount){
            this.queries = queriesCount;
        }

        @Override
        public void run(SourceContext<Double> ctx) throws Exception {
            Random random = new Random();

            for (int i = 0; i < queries; i++) {
                long startTs = System.currentTimeMillis();
                while (System.currentTimeMillis() < startTs + 1000) {
                    // active waiting
                }
                ctx.collectWithTimestamp(random.nextDouble(), System.currentTimeMillis());
            }
        }

        @Override
        public void cancel() { }
    }

    private static class CustomCoProcessFunction extends CoProcessFunction<DDSketch, Double, Tuple2<Double, Double>> {

        // private transient ListState<DDSketch> latestSketch;
        ArrayList<Double> queryList = new ArrayList<>();
        DDSketch current;

        @Override
        public void processElement1(DDSketch value, Context ctx, Collector<Tuple2<Double, Double>> out) throws Exception {
            if (current == null && queryList.size() > 0) {
                for (double quantile: queryList){
                    out.collect(new Tuple2<>(quantile, value.getValueAtQuantile(quantile)));
                }
            }
            current = value;
        }

        @Override
        public void processElement2(Double value, Context ctx, Collector<Tuple2<Double, Double>> out) throws Exception {
            if (current == null){
                queryList.add(value);
            } else {
                out.collect(new Tuple2<>(value, current.getValueAtQuantile(value)));
            }
        }
    }

    private static class CustomTimeStampCoProcessFunction extends CoProcessFunction<Tuple3<DDSketch, Long, Long>, Double, Tuple3<Double, Double, Long>> {
        // private transient ListState<DDSketch> latestSketch;
        ArrayList<Double> queryList = new ArrayList<>();
        Tuple3<DDSketch, Long, Long> current;

        @Override
        public void processElement1(Tuple3<DDSketch, Long, Long> value, Context ctx, Collector<Tuple3<Double, Double, Long>> out) throws Exception {
            if (current == null && queryList.size() > 0) {
                for (double quantile: queryList){
                    out.collect(new Tuple3<>(quantile, value.f0.getValueAtQuantile(quantile), value.f1));
                }
            }
            current = value;
        }

        @Override
        public void processElement2(Double value, Context ctx, Collector<Tuple3<Double, Double, Long>> out) throws Exception {
            if (current == null){
                queryList.add(value);
            } else {
                out.collect(new Tuple3<>(value, current.f0.getValueAtQuantile(value), current.f1));
            }
        }
    }

    private static class CurrentTimeAndWatermarks implements AssignerWithPeriodicWatermarks<Double>{

        private long currentMaxTimestamp;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp);
        }

        @Override
        public long extractTimestamp(Double element, long previousElementTimestamp) {
            currentMaxTimestamp = System.currentTimeMillis() + 60000; // + 1 minute
            return currentMaxTimestamp;
        }
    }


    private static void runWithIntervalJoin(SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped, StreamExecutionEnvironment env, BuildSynopsisConfig config, Object... params){

        SingleOutputStreamOperator<Tuple3<DDSketch, Long, Long>> timestampedSketches = NewBuildSynopsis
                .timeBasedWithWindowTimes(timestamped, DDSketch.class, config, params)
                .returns(new TypeHint<Tuple3<DDSketch, Long, Long>>() {});

        KeyedStream<Tuple4<DDSketch, Long, Long, Long>, Tuple> sketchWithKey = timestampedSketches.map(new MapFunction<Tuple3<DDSketch, Long, Long>, Tuple4<DDSketch, Long, Long, Long>>() {
            @Override
            public Tuple4<DDSketch, Long, Long, Long> map(Tuple3<DDSketch, Long, Long> value) throws Exception {
                return new Tuple4<>(value.f0, value.f1, value.f2, value.f1 / 60000);
            }
        }).keyBy(3);

        // timestampedSketches.writeAsText("/Users/joschavonhein/Workspace/scotty-window-processor/EDADS/Results/timestampedSketches.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

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
