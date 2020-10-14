package Benchmark;

import ApproximateDataAnalytics.*;
import Benchmark.Functions.Stratifier;
import Benchmark.Functions.TimestampsAndWatermarks;
import Benchmark.Sources.*;
import FlinkScottyConnector.BuildStratifiedSynopsis;
import FlinkScottyConnector.BuildSynopsisConfig;
import FlinkScottyConnector.NewBuildSynopsis;
import Jobs.ExampleStratifiedADAJob;
import Synopsis.Sketches.DDSketch;
import Synopsis.StratifiedSynopsisWrapper;
import Synopsis.WindowedSynopsis;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

/**
 * Used to Benchmark the Approximate Data Analytics Package.
 *
 * It will run queryLatest, queryStratified, queryTimestamped and queryStratifiedTimestamped a number of times with a given
 * Synopsis to measure the throughput in the queryStream for varying parallelism.
 *
 *
 * @author Joscha von Hein
 */
public class ADABenchmark {

    public static void main(String[] args){
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final String outputDir = parameterTool.get("outputDir", null);
        final boolean test = parameterTool.has("test");
        int maxParallelism = test ? 2 : 256;
        final int stratification = 10;
        final int sketchTroughput = 1000; // # tuples / seconds to build the sketch
        final List<Tuple2<Long, Long>> gaps = new ArrayList<>();
        final double accuracy = 0.01; // relative accuracy of DD-Sketch
        final int maxNumberOfBins = 500; // maximum number of bins of DD-Sketch
        final Object[] params = new Object[]{accuracy, maxNumberOfBins};
        final Time windowTime = Time.seconds(5);
        final BuildSynopsisConfig config = new BuildSynopsisConfig(windowTime, null, 0); // config object for tumbling window with 5 seconds slide time

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().enableObjectReuse();
        env.setParallelism(env.getMaxParallelism());


        // initial parallelism is set to 256 to make the last measurement - change if needed
        for (int parallelism = 256; parallelism <= maxParallelism; parallelism *= 2) {
            for (int queryThroughput = 400000; queryThroughput <= 500000; queryThroughput += 100000) {
                for (int iteration = 0; iteration < 10; iteration++) {
                    env.setParallelism(parallelism);
                    String configString = "ADA_Benchmark;parallelism;" + parallelism + ";targetQueryThroughput;" + queryThroughput;
                    System.out.println(configString);
                    runQueryTimestamped(configString, outputDir,env, sketchTroughput, queryThroughput, gaps, config, params);
                    runQueryStratifiedTimestamped(configString, outputDir,env, sketchTroughput, queryThroughput, stratification, gaps, config, params);
                }
            }
            for (int queryThroughput = 800000; queryThroughput <= 1000000 ; queryThroughput += 200000) {
                for (int iteration = 0; iteration < 10; iteration++) {
                    env.setParallelism(parallelism);
                    String configString = "ADA_Benchmark;parallelism;" + parallelism + ";targetQueryThroughput;" + queryThroughput;
                    System.out.println(configString);
                    runQueryLatest(configString, outputDir, env, sketchTroughput, queryThroughput, gaps, config, params);
                    runQueryStratifiedLatest(configString, outputDir,env, sketchTroughput, queryThroughput, stratification, gaps, config, params);
                }
            }
        }

    }


    static JobExecutionResult runQueryLatest(String configString, String outputDir,StreamExecutionEnvironment env, int sketchThroughput, int queryThroughput, List<Tuple2<Long, Long>> gaps, BuildSynopsisConfig config, Object... params){
        // synopsis stream runtime is 20 seconds, queryStream waiting time is 40 seconds and after that 20 seconds query stream runtime
        // -> 60 seconds total runtime  for all methods

        DataStreamSource<Tuple3<Integer, Integer, Long>> messageStream = env.addSource(new UniformDistributionSource(20000, sketchThroughput, gaps));
        final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
                .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());

        final DataStream<WindowedSynopsis<DDSketch>> synopsisStream = NewBuildSynopsis.timeBasedWithWindowTimes(timestamped, DDSketch.class, config, params);

        final DataStreamSource<Double> queryStreamSource = env.addSource(new SimpleQuerySource(Time.seconds(20), queryThroughput, Time.seconds(40)));
        final SingleOutputStreamOperator<Double> queryStream = queryStreamSource.flatMap(new ParallelThroughputLogger<Double>(1000, configString + ";latest"));

        final QueryFunction<Double, DDSketch, Double> queryFunction = new QueryFunction<Double, DDSketch, Double>() {
            @Override
            public Double query(Double query, DDSketch synopsis) {
                return synopsis.getValueAtQuantile(query);
            }
        };

        final SingleOutputStreamOperator<QueryResult<Double, Double>> queryResults = ApproximateDataAnalytics.queryLatest(synopsisStream, queryStream, queryFunction);

        if (outputDir == null){
            queryResults.addSink(new SinkFunction<QueryResult<Double, Double>>() {
                @Override
                public void invoke(QueryResult<Double, Double> value, Context context) throws Exception {
                    // empty sink;
                }
            });
        }else {
            queryResults.writeAsText(outputDir+"/query_latest.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        }


        try {
            return env.execute("Query Latest Job");
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    static JobExecutionResult runQueryStratifiedLatest(String configString, String outputDir, StreamExecutionEnvironment env, int sketchThroughput, int queryThroughput, int stratification, List<Tuple2<Long, Long>> gaps, BuildSynopsisConfig config, Object... params){

        Stratifier stratifier = new Stratifier(stratification);

        DataStreamSource<Tuple3<Integer, Integer, Long>> messageStream = env.addSource(new UniformDistributionSource(20000, sketchThroughput, gaps));
        final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
                .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());

        DataStream<StratifiedSynopsisWrapper<Integer, WindowedSynopsis<DDSketch>>> stratifiedSynopsisStream = BuildStratifiedSynopsis.timeBasedADA(timestamped, config.getWindowTime(), config.getSlideTime(), stratifier, DDSketch.class, params);

        final DataStreamSource<Tuple2<Integer, Double>> queryStreamSource = env.addSource(new StratifiedQuerySource(Time.seconds(20), queryThroughput, Time.seconds(40), stratification));
        final SingleOutputStreamOperator<Tuple2<Integer, Double>> queryStream = queryStreamSource.flatMap(new ParallelThroughputLogger<Tuple2<Integer, Double>>(1000, configString + ";stratified_latest"));

        QueryFunction<Double, DDSketch, Double> queryFunction = new QueryFunction<Double, DDSketch, Double>() {
            @Override
            public Double query(Double query, DDSketch synopsis) {
                return synopsis.getValueAtQuantile(query);
            }
        };

        DataStream<StratifiedQueryResult<Double, Double, Integer>> queryResultDataStream = ApproximateDataAnalytics.queryLatestStratified(stratifiedSynopsisStream, queryStream, queryFunction, Integer.class);

        if (outputDir == null){
            queryResultDataStream.addSink(new SinkFunction<StratifiedQueryResult<Double, Double, Integer>>() {
                @Override
                public void invoke(StratifiedQueryResult<Double, Double, Integer> value, Context context) throws Exception {
                    // do nothing sink;
                }
            });
        }else {
            queryResultDataStream.writeAsText(outputDir+"/query_stratified_latest.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        }

        try {
            return env.execute("Query Latest Job");
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    static JobExecutionResult runQueryTimestamped(String configString, String outputDir, StreamExecutionEnvironment env, int sketchThroughput, int queryThroughput, List<Tuple2<Long, Long>> gaps, BuildSynopsisConfig config, Object... params){

        DataStreamSource<Tuple3<Integer, Integer, Long>> messageStream = env.addSource(new UniformDistributionSource(20000, sketchThroughput, gaps));

        final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
                .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());

        DataStream<WindowedSynopsis<DDSketch>> synopsesStream = NewBuildSynopsis.timeBasedWithWindowTimes(timestamped, DDSketch.class, config, params);

        DataStream<TimestampedQuery<Double>> timestampedQueriesSource = env.addSource(new TimestampedQuerySource(Time.seconds(20), Time.seconds(40), queryThroughput, Time.seconds(20)));
        final SingleOutputStreamOperator<TimestampedQuery<Double>> timestampedQueries = timestampedQueriesSource.flatMap(new ParallelThroughputLogger<TimestampedQuery<Double>>(1000, configString + ";timestamped"));

        QueryFunction<TimestampedQuery<Double>, WindowedSynopsis<DDSketch>, QueryResult<TimestampedQuery<Double>, Double>> queryFunction =
                new QueryFunction<TimestampedQuery<Double>, WindowedSynopsis<DDSketch>, QueryResult<TimestampedQuery<Double>, Double>>() {
                    @Override
                    public QueryResult<TimestampedQuery<Double>, Double> query(TimestampedQuery<Double> query, WindowedSynopsis<DDSketch> synopsis) {
                        return new QueryResult<TimestampedQuery<Double>, Double>(synopsis.getSynopsis().getValueAtQuantile(query.getQuery()), query, synopsis);
                    }
                };

        final SingleOutputStreamOperator<QueryResult<TimestampedQuery<Double>, Double>> queryResults =
                ApproximateDataAnalytics.queryTimestamped(synopsesStream, timestampedQueries, queryFunction, 120);

        if (outputDir == null){
            queryResults.addSink(new SinkFunction<QueryResult<TimestampedQuery<Double>, Double>>() {
                @Override
                public void invoke(QueryResult<TimestampedQuery<Double>, Double> value, Context context) throws Exception {
                    // do nothing sink;
                }
            });
        }else {
            queryResults.writeAsText(outputDir+"/query_timestamped.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        }

        try {
            return env.execute("Query Timestamped Job");
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    static JobExecutionResult runQueryStratifiedTimestamped(String configString, String outputDir, StreamExecutionEnvironment env, int sketchThroughput, int queryThroughput, int stratification, List<Tuple2<Long, Long>> gaps, BuildSynopsisConfig config, Object... params){

        DataStreamSource<Tuple3<Integer, Integer, Long>> messageStream = env.addSource(new UniformDistributionSource(20000, sketchThroughput, gaps));

        final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
                .assignTimestampsAndWatermarks(new ExampleStratifiedADAJob.TimestampsAndWatermarks());

        SingleOutputStreamOperator<StratifiedSynopsisWrapper<Integer, WindowedSynopsis<DDSketch>>> stratifiedSynopsisStream = BuildStratifiedSynopsis
                .timeBasedADA(timestamped, config.getWindowTime(), config.getSlideTime(), new Stratifier(stratification), DDSketch.class, params);

        DataStream<Tuple2<Integer, TimestampedQuery<Double>>> queryStreamSource = env.addSource(new StratifiedTimestampedQuerySource(queryThroughput, Time.seconds(40), Time.seconds(20), Time.seconds(20), stratification));
        final SingleOutputStreamOperator<Tuple2<Integer, TimestampedQuery<Double>>> queryStream = queryStreamSource.flatMap(new ParallelThroughputLogger<Tuple2<Integer, TimestampedQuery<Double>>>(1000, configString + ";stratified_timestamped"));

        QueryFunction<Tuple2<Integer, TimestampedQuery<Double>>, WindowedSynopsis<DDSketch>, StratifiedQueryResult<TimestampedQuery<Double>, Double, Integer>> queryFunction =
                new QueryFunction<Tuple2<Integer, TimestampedQuery<Double>>, WindowedSynopsis<DDSketch>, StratifiedQueryResult<TimestampedQuery<Double>, Double, Integer>>() {
                    @Override
                    public StratifiedQueryResult<TimestampedQuery<Double>, Double, Integer> query(Tuple2<Integer, TimestampedQuery<Double>> query, WindowedSynopsis<DDSketch> synopsis) {
                        Double result = synopsis.getSynopsis().getValueAtQuantile(query.f1.getQuery());
                        return new StratifiedQueryResult<TimestampedQuery<Double>, Double, Integer>(result, query, synopsis);
                    }
                };

        final SingleOutputStreamOperator<StratifiedQueryResult<TimestampedQuery<Double>, Double, Integer>> queryResultStream =
                ApproximateDataAnalytics.queryTimestampedStratified(stratifiedSynopsisStream, queryStream, queryFunction, Integer.class, 100);

        if (outputDir == null){
            queryResultStream.addSink(new SinkFunction<StratifiedQueryResult<TimestampedQuery<Double>, Double, Integer>>() {
                @Override
                public void invoke(StratifiedQueryResult<TimestampedQuery<Double>, Double, Integer> value, Context context) throws Exception {
                    // do nothing sink;
                }
            });
        }else {
            queryResultStream.writeAsText(outputDir+"/query_timestamped_stratified.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        }

        try {
            env.execute("Query Timestamped Stratified Job");
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }
}
