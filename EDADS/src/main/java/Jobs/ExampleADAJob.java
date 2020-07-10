package Jobs;


import ApproximateDataAnalytics.ApproximateDataAnalytics;
import Benchmark.Sources.UniformDistributionSource;
import FlinkScottyConnector.BuildSynopsisConfig;
import FlinkScottyConnector.NewBuildSynopsis;
import Synopsis.Sketches.DDSketch;
import Synopsis.WindowedSynopsis;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import ApproximateDataAnalytics.QueryFunction;
import ApproximateDataAnalytics.QueryResult;

public class ExampleADAJob {
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

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // DataStreamSource<Tuple3<Integer, Integer, Long>> messageStream = env.addSource(new ZipfDistributionSource(pathToZipfData, runtime, throughput, gaps));
        DataStreamSource<Tuple3<Integer, Integer, Long>> messageStream = env.addSource(new UniformDistributionSource(runtime, throughput, gaps));

        final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
                .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());

        SingleOutputStreamOperator<WindowedSynopsis<DDSketch>> synopsesStream = NewBuildSynopsis.timeBasedWithWindowTimes(timestamped, DDSketch.class, config, params);

        SingleOutputStreamOperator<Double> queryStream = env.addSource(new QuerySource(20));

        SingleOutputStreamOperator<QueryResult<Double, Double>> queryResults = ApproximateDataAnalytics.queryLatest(synopsesStream, queryStream, new DDSketchQuery());

        queryResults.writeAsText("EDADS/output/exampleADA.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("ADA Example Job");
    }

    private static class DDSketchQuery implements QueryFunction<Double, DDSketch, Double>{

        @Override
        public Double query(Double query, DDSketch ddSketch) {
            return ddSketch.getValueAtQuantile(query);
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
