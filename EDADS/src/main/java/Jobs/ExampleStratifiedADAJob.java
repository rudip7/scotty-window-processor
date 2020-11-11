package Jobs;


import ApproximateDataAnalytics.ApproximateDataAnalytics;
import ApproximateDataAnalytics.QueryFunction;
import Benchmark.Sources.UniformDistributionSource;
import FlinkScottyConnector.BuildStratifiedSynopsis;
import FlinkScottyConnector.Configs.BuildSynopsisConfig;
import Synopsis.Sketches.DDSketch;
import Synopsis.StratifiedSynopsisWrapper;
import Synopsis.WindowedSynopsis;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import ApproximateDataAnalytics.StratifiedQueryResult;

public class ExampleStratifiedADAJob {
    public static void main(String[] args) throws Exception{

        // Arguments & setup
        final int runtime = 10000; // runtime in milliseconds
        final int throughput = 100; // target throughput
        final List<Tuple2<Long, Long>> gaps = new ArrayList<>();
        final double accuracy = 0.01; // relative accuracy of DD-Sketch
        final int maxNumberOfBins = 2000; // maximum number of bins of DD-Sketch
//        final String pathToZipfData = "/Users/joschavonhein/Data/zipfTimestamped.gz";
        Object[] params = new Object[]{accuracy, maxNumberOfBins};
        BuildSynopsisConfig config = new BuildSynopsisConfig(Time.seconds(6L), Time.seconds(3L), 0);

        int stratification = 4;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // DataStreamSource<Tuple3<Integer, Integer, Long>> messageStream = env.addSource(new ZipfDistributionSource(pathToZipfData, runtime, throughput, gaps));
        DataStreamSource<Tuple3<Integer, Integer, Long>> messageStream = env.addSource(new UniformDistributionSource(runtime, throughput, gaps));

        final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
                .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());

        SingleOutputStreamOperator<StratifiedSynopsisWrapper<Integer, WindowedSynopsis<DDSketch>>> stratifiedSynopsesStream = BuildStratifiedSynopsis.timeBasedADA(timestamped, Time.seconds(6L), Time.seconds(3L), new Stratifier(stratification), DDSketch.class, params);

//        stratifiedSynopsesStream.writeAsText("EDADS/output/exampleADAStratified.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        DataStreamSource<Tuple2<Integer, Double>> queryStream = env.addSource(new StratifiedQuerySource(20, stratification));

//        KeyedStream<StratifiedQuery<Double, Integer>, Integer> keyedQueryStream = queryStream.keyBy(new StratifiedQuery.StratifiedQueryKeySelector<Double, Integer>());

        SingleOutputStreamOperator<StratifiedQueryResult<Double, Double, Integer>> queryResults = ApproximateDataAnalytics.queryLatestStratified(stratifiedSynopsesStream, queryStream, new DDSketchQuery(), Integer.class);

        queryResults.writeAsText("EDADS/Results/exampleADAStratified.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("ADA Example Job");
    }

    private static class DDSketchQuery implements QueryFunction<Double, DDSketch, Double>{

        @Override
        public Double query(Double query, DDSketch ddSketch) {
            return ddSketch.getValueAtQuantile(query);
        }
    }

    public static class Stratifier extends RichMapFunction<Tuple3<Integer, Integer, Long>, Tuple2<Integer, Integer>> {

        private int stratification;

        public Stratifier(int stratification) {
            this.stratification = stratification;
        }

        @Override
        public Tuple2<Integer, Integer> map(Tuple3<Integer, Integer, Long> value) throws Exception {
            int key = (int)(value.f0 / 100d * stratification);
            if (key >= stratification){
                key = stratification -1;
            }
            return new Tuple2<>(key, value.f0);
        }

//        @Override
//        public void open(Configuration parameters) throws Exception {
//            super.open(parameters);
//            ExecutionConfig.GlobalJobParameters globalParams = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
//            Configuration globConf = (Configuration) globalParams;
//            stratification = globConf.getInteger("stratification", 1);
//        }
    }


    private static class StratifiedQuerySource implements SourceFunction<Tuple2<Integer, Double>> {

        private final int queries;
        private final int stratification;


        public StratifiedQuerySource(int queriesCount, int stratification){
            this.queries = queriesCount;
            this.stratification = stratification;
        }

        @Override
        public void run(SourceContext<Tuple2<Integer, Double>> ctx) throws Exception {
            Random random = new Random();

            for (int i = 0; i < queries; i++) {
                long startTs = System.currentTimeMillis();
                while (System.currentTimeMillis() < startTs + 1000) {
                    // active waiting
                }
                ctx.collectWithTimestamp(new Tuple2<>(random.nextInt(stratification), random.nextDouble()), System.currentTimeMillis());
            }
        }

        @Override
        public void cancel() { }
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
