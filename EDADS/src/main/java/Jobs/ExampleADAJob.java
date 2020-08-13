package Jobs;


import ApproximateDataAnalytics.ApproximateDataAnalytics;
import ApproximateDataAnalytics.TimestampedQuery;
import ApproximateDataAnalytics.QueryFunction;
import ApproximateDataAnalytics.QueryResult;
import Benchmark.Sources.UniformDistributionSource;
import FlinkScottyConnector.BuildSynopsisConfig;
import FlinkScottyConnector.NewBuildSynopsis;
import Synopsis.Sketches.DDSketch;
import Synopsis.WindowedSynopsis;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
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

public class ExampleADAJob {
    public static void main(String[] args) throws Exception{

        // Arguments & setup
        final int runtime = 30000; // runtime in milliseconds
        final int throughput = 1000; // target throughput
        final List<Tuple2<Long, Long>> gaps = new ArrayList<>();
        final double accuracy = 0.01; // relative accuracy of DD-Sketch
        final int maxNumberOfBins = 500; // maximum number of bins of DD-Sketch
//        final String pathToZipfData = "/Users/joschavonhein/Data/zipfTimestamped.gz";
        Object[] params = new Object[]{accuracy, maxNumberOfBins};
        BuildSynopsisConfig config = new BuildSynopsisConfig(Time.seconds(5), null, 0);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // DataStreamSource<Tuple3<Integer, Integer, Long>> messageStream = env.addSource(new ZipfDistributionSource(pathToZipfData, runtime, throughput, gaps));
        DataStreamSource<Tuple3<Integer, Integer, Long>> messageStream = env.addSource(new UniformDistributionSource(runtime, throughput, gaps));

        final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
                .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());

        DataStream<WindowedSynopsis<DDSketch>> synopsesStream = NewBuildSynopsis.timeBasedWithWindowTimes(timestamped, DDSketch.class, config, params);

        DataStream<TimestampedQuery<Double>> timestampedQueries = env.addSource(new TimestampedQuerySource(200, 5));


        QueryFunction<TimestampedQuery<Double>, WindowedSynopsis<DDSketch>, QueryResult<TimestampedQuery<Double>, Double>> queryFunction =
                new QueryFunction<TimestampedQuery<Double>, WindowedSynopsis<DDSketch>, QueryResult<TimestampedQuery<Double>, Double>>() {
            @Override
            public QueryResult<TimestampedQuery<Double>, Double> query(TimestampedQuery<Double> query, WindowedSynopsis<DDSketch> synopsis) {
                return new QueryResult<TimestampedQuery<Double>, Double>(synopsis.getSynopsis().getValueAtQuantile(query.getQuery()), query, synopsis);
            }
        };

        SingleOutputStreamOperator<QueryResult<TimestampedQuery<Double>, Double>> queryResults = ApproximateDataAnalytics.queryTimestamped(synopsesStream, timestampedQueries, queryFunction, 200);


        queryResults.writeAsText("EDADS/output/timestamped_query_results.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("ADA Example Job");
    }

    private static class TimestampedQuerySource implements SourceFunction<TimestampedQuery<Double>> {

        private final int queries;
        private final int wait;

        public TimestampedQuerySource(int queries, int secondsWait) {
            this.wait = secondsWait;
            this.queries = queries;
        }

        @Override
        public void run(SourceContext<TimestampedQuery<Double>> ctx) throws Exception {
            Random random = new Random();

            long startTs = System.currentTimeMillis();
            while (System.currentTimeMillis() < startTs + wait * 1000) {
                // active waiting
            }

            for (int i = 0; i < queries; i++) {
                startTs = System.currentTimeMillis();
                while (System.currentTimeMillis() < startTs + 100) {
                    // active waiting
                }
                long timestamp = System.currentTimeMillis();
                ctx.collectWithTimestamp(new TimestampedQuery<Double>(random.nextDouble(), timestamp - 10000), timestamp);
            }
        }

        @Override
        public void cancel() {

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
