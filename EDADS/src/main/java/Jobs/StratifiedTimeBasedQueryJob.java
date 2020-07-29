package Jobs;

import ApproximateDataAnalytics.ApproximateDataAnalytics;
import ApproximateDataAnalytics.TimestampedQuery;
import ApproximateDataAnalytics.QueryResult;
import ApproximateDataAnalytics.QueryFunction;
import Benchmark.Sources.UniformDistributionSource;
import FlinkScottyConnector.BuildStratifiedSynopsis;
import FlinkScottyConnector.BuildSynopsisConfig;
import Synopsis.Sketches.DDSketch;
import Synopsis.StratifiedSynopsisWrapper;
import Synopsis.WindowedSynopsis;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class StratifiedTimeBasedQueryJob {

    public static void main(String[] args){
        // Arguments & setup
        final int runtime = 30000; // runtime in milliseconds
        final int throughput = 10000; // target throughput
        final List<Tuple2<Long, Long>> gaps = new ArrayList<>();
        final double accuracy = 0.01; // relative accuracy of DD-Sketch
        final int maxNumberOfBins = 2000; // maximum number of bins of DD-Sketch
//        final String pathToZipfData = "/Users/joschavonhein/Data/zipfTimestamped.gz";
        Object[] params = new Object[]{accuracy, maxNumberOfBins};
        BuildSynopsisConfig config = new BuildSynopsisConfig(Time.seconds(10), Time.seconds(5), 0);

        int stratification = 10;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // DataStreamSource<Tuple3<Integer, Integer, Long>> messageStream = env.addSource(new ZipfDistributionSource(pathToZipfData, runtime, throughput, gaps));
        DataStreamSource<Tuple3<Integer, Integer, Long>> messageStream = env.addSource(new UniformDistributionSource(runtime, throughput, gaps));

        final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
                .assignTimestampsAndWatermarks(new ExampleStratifiedADAJob.TimestampsAndWatermarks());

        SingleOutputStreamOperator<StratifiedSynopsisWrapper<Integer, WindowedSynopsis<DDSketch>>> stratifiedSynopsisStream = BuildStratifiedSynopsis
                .timeBasedADA(timestamped, Time.seconds(6L), Time.seconds(3L), new Stratifier(stratification), DDSketch.class, params);


        DataStream<Tuple2<Integer, TimestampedQuery<Double>>> queryStream = env.addSource(new StratifiedTimestampedQuerySource(2000, 10, 10, 30));

        QueryFunction<TimestampedQuery<Double>, WindowedSynopsis<DDSketch>, QueryResult<TimestampedQuery<Double>, Double>> queryFunction = new QueryFunction<TimestampedQuery<Double>, WindowedSynopsis<DDSketch>, QueryResult<TimestampedQuery<Double>, Double>>() {
            @Override
            public QueryResult<TimestampedQuery<Double>, Double> query(TimestampedQuery<Double> query, WindowedSynopsis<DDSketch> synopsis) {

                Double result = synopsis.getSynopsis().getValueAtQuantile(query.getQuery());
                return new QueryResult<TimestampedQuery<Double>, Double>(result, query, synopsis);
            }
        };


        final SingleOutputStreamOperator<QueryResult<TimestampedQuery<Double>, Double>> queryResultStream =
                ApproximateDataAnalytics.queryTimestampedStratified(stratifiedSynopsisStream, queryStream, queryFunction, Integer.class, 100);

        queryResultStream.print();

        queryResultStream.writeAsText("/Users/joschavonhein/Workspace/scotty-window-processor/EDADS/Results/timebased_stratified_query_results.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        try {
            env.execute("Stratified Timestamped Query ADA Job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static class Stratifier extends RichMapFunction<Tuple3<Integer, Integer, Long>, Tuple2<Integer, Integer>> {

        private int stratification;

        public Stratifier(int stratification) {
            this.stratification = stratification;
        }

        @Override
        public Tuple2<Integer, Integer> map(Tuple3<Integer, Integer, Long> value) throws Exception {
            // creates a key value on which to stratify based on the first value in the tuple. - basically creating a new vlaue to keyBy...

            int key = (int)(value.f0 / 100d * stratification);
            if (key >= stratification){
                key = stratification -1;
            }
            return new Tuple2<>(key, value.f0);
        }
    }

    private static class StratifiedTimestampedQuerySource implements SourceFunction<Tuple2<Integer, TimestampedQuery<Double>>> {

        private final int queries;
        private final int wait;
        private final int stratification;
        private final int runtime;

        public StratifiedTimestampedQuerySource(int queries, int secondsWait, int stratification, int runtime) {
            this.wait = secondsWait;
            this.queries = queries;
            this.stratification = stratification;
            this.runtime = runtime;
        }

        @Override
        public void run(SourceContext<Tuple2<Integer, TimestampedQuery<Double>>> ctx) throws Exception {
            Random random = new Random();

            long startTs = System.currentTimeMillis();
            while (System.currentTimeMillis() < startTs + wait * 1000) {
                // active waiting
            }

            for (int i = 0; i < queries; i++) {
                long latest = System.currentTimeMillis();
                long increment = (runtime - wait) * 1000 / queries;
                while (System.currentTimeMillis() < latest + increment) {
                    // active waiting
                }
                long timestamp = System.currentTimeMillis();
                int bound = random.nextInt((int)((timestamp - startTs) / 1000)); // upper bound in seconds from job start
                long query_timestamp = startTs + bound * 1000;
                TimestampedQuery<Double> timestampedQuery = new TimestampedQuery<Double>(random.nextDouble(), query_timestamp);
                ctx.collectWithTimestamp(new Tuple2<>(random.nextInt(stratification), timestampedQuery), timestamp);
            }
        }

        @Override
        public void cancel() {

        }
    }
}
