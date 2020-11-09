package Benchmark;

import ApproximateDataAnalytics.*;
import Benchmark.Functions.Stratifier;
import Benchmark.Functions.TimestampsAndWatermarks;
import Benchmark.Sources.*;
import FlinkScottyConnector.BuildStratifiedSynopsis;
import FlinkScottyConnector.BuildSynopsisConfig;
import FlinkScottyConnector.NewBuildSynopsis;
import Jobs.ExampleStratifiedADAJob;
import Synopsis.Sketches.CountMinSketch;
import Synopsis.Sketches.DDSketch;
import Synopsis.StratifiedSynopsisWrapper;
import Synopsis.WindowedSynopsis;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
import org.omg.CORBA.INTERNAL;

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
        final boolean latest = parameterTool.has("l");
        final boolean timestamped = parameterTool.has("t");
        final boolean latestStratified = parameterTool.has("ls");
        final boolean timestampedStratified = parameterTool.has("ts");
        int maxParallelism = test ? 2 : 128;
        final int stratification = 10;
        final int sketchThroughput = 1000; // # tuples / seconds to build the sketch
        final Integer width = test ? 128 : 512; // width of the Count Min Sketch -> high to reduce #collisions
        final Integer depth = test ? 32 : 16; // depths of the Count Min Sketch
        final Long seed = 42L;
        final Object[] params = new Object[]{width, depth, seed}; // sketch parameters
        final Time windowTime = Time.seconds(5);
        final BuildSynopsisConfig config = new BuildSynopsisConfig(windowTime, null, 0); // config object for tumbling window with 5 seconds window time

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().enableObjectReuse();
        env.setParallelism(env.getMaxParallelism());

        final int queryThroughputLatest = test ? 1000: 1000000;
        final int queryThroughputTimestamped = test ? 1000 :250000;
        final int iterations = test ? 1 : 10;

        /**
         * max target query Throughputs:
         *
         * latest: 1,000,000
         * latest_stratified: 800,000
         * timestamped: 250,000
         * timestamped_stratified: 200,000
         *
         * -> latest queries: 1,000,000
         * -> timestamped queries: 250,000
         */
        for (int parallelism = 128; parallelism <= maxParallelism; parallelism *= 2) {
            env.setParallelism(parallelism);

            String configStringLatest = "ADA_Benchmark;parallelism;" + parallelism + ";targetQueryThroughput;" + queryThroughputLatest;
            String configStringTimestamped = "ADA_Benchmark;parallelism;" + parallelism + ";targetQueryThroughput;" + queryThroughputTimestamped;
            if(latestStratified){
                for (int iteration = 0; iteration < iterations; iteration++) {
                    System.out.println(configStringLatest+";latest_stratified");
                    runQueryStratifiedLatest(configStringLatest, outputDir,env, sketchThroughput, queryThroughputLatest, stratification, config, params);
                }
            }
            if (latest) {
                for (int iteration = 0; iteration < iterations; iteration++) {
                    System.out.println(configStringLatest + ";latest");
                    runQueryLatest(configStringLatest, outputDir, env, sketchThroughput, queryThroughputLatest, config, params);
                }
            }
            if (timestamped) {
                for (int iteration = 0; iteration < iterations; iteration++) {
                    System.out.println(configStringTimestamped + ";timestamped");
                    runQueryTimestamped(configStringTimestamped, outputDir, env, sketchThroughput, queryThroughputTimestamped, config, params);
                }
            }
            if(timestampedStratified){
                for (int iteration = 0; iteration < iterations; iteration++) {
                    System.out.println(configStringTimestamped+";timestamped_stratified");
                    runQueryStratifiedTimestamped(configStringTimestamped, outputDir, env, sketchThroughput, queryThroughputTimestamped, stratification, config, params);
                }
            }
        }
    }


    static JobExecutionResult runQueryLatest(String configString, String outputDir,StreamExecutionEnvironment env, int sketchThroughput, int queryThroughput, BuildSynopsisConfig config, Object... params){
        // synopsis stream runtime is 20 seconds, queryStream waiting time is 40 seconds and after that 20 seconds query stream runtime
        // -> 60 seconds total runtime  for all methods

        final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> sourceStream = env.addSource(new IP_Address_Source(20000, sketchThroughput))
                .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());

        final DataStream<WindowedSynopsis<CountMinSketch>> synopsisStream = NewBuildSynopsis.timeBasedWithWindowTimes(sourceStream, CountMinSketch.class, config, params);

        final SingleOutputStreamOperator<Integer> queryStream = env.addSource(new SimpleQuerySource(Time.seconds(20), queryThroughput, Time.seconds(40)))
                .flatMap(new ParallelThroughputLogger<Integer>(1000, configString + ";latest"));

        final QueryFunction<Integer, CountMinSketch, Integer> queryFunction = new QueryFunction<Integer, CountMinSketch, Integer>() {
            @Override
            public Integer query(Integer query, CountMinSketch synopsis) {
                return synopsis.query(query); // TODO: possible error: unchecked call to .query(T) as we know T is an Integer
            }
        };

        final SingleOutputStreamOperator<QueryResult<Integer, Integer>> queryResults = ApproximateDataAnalytics.queryLatest(synopsisStream, queryStream, queryFunction);

        if (outputDir == null){
            queryResults.addSink(new SinkFunction<QueryResult<Integer, Integer>>() {
                @Override
                public void invoke(QueryResult<Integer, Integer> value, Context context) throws Exception {
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

    static JobExecutionResult runQueryStratifiedLatest(String configString, String outputDir, StreamExecutionEnvironment env, int sketchThroughput, int queryThroughput, int stratification, BuildSynopsisConfig config, Object... params){

        Stratifier stratifier = new Stratifier(stratification);

        final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> sourceStream = env.addSource(new IP_Address_Source(20000, sketchThroughput))
                .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());

        DataStream<StratifiedSynopsisWrapper<Integer, WindowedSynopsis<CountMinSketch>>> stratifiedSynopsisStream = BuildStratifiedSynopsis.timeBasedADA(sourceStream, config.getWindowTime(), config.getSlideTime(), stratifier, CountMinSketch.class, params);

        final SingleOutputStreamOperator<Tuple2<Integer, Integer>> queryStream = env.addSource(new StratifiedQuerySource(Time.seconds(20), queryThroughput, Time.seconds(40), stratification))
                .flatMap(new ParallelThroughputLogger<Tuple2<Integer, Integer>>(1000, configString + ";stratified_latest"));

        final QueryFunction<Integer, CountMinSketch, Integer> queryFunction = new QueryFunction<Integer, CountMinSketch, Integer>() {
            @Override
            public Integer query(Integer query, CountMinSketch synopsis) {
                return synopsis.query(query);
            }
        };

        final SingleOutputStreamOperator<StratifiedQueryResult<Integer, Integer, Integer>> queryResultStream = ApproximateDataAnalytics.queryLatestStratified(stratifiedSynopsisStream, queryStream, queryFunction, Integer.class);

        if (outputDir == null){
            queryResultStream.addSink(new SinkFunction<StratifiedQueryResult<Integer, Integer, Integer>>() {
                @Override
                public void invoke(StratifiedQueryResult<Integer, Integer, Integer> value, Context context) throws Exception {
                    // do nothing sink;
                }
            });
        }else {
            queryResultStream.writeAsText(outputDir+"/query_stratified_latest.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        }

        try {
            return env.execute("Query Latest Stratified Job");
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    static JobExecutionResult runQueryTimestamped(String configString, String outputDir, StreamExecutionEnvironment env, int sketchThroughput, int queryThroughput, BuildSynopsisConfig config, Object... params){

        final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> sourceStream = env.addSource(new IP_Address_Source(20000, sketchThroughput))
                .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());

        final DataStream<WindowedSynopsis<CountMinSketch>> synopsesStream = NewBuildSynopsis.timeBasedWithWindowTimes(sourceStream, CountMinSketch.class, config, params);

        DataStream<TimestampedQuery<Integer>> timestampedQueries = env.addSource(new TimestampedQuerySource(Time.seconds(20), Time.seconds(40), queryThroughput, Time.seconds(20)))
                .flatMap(new ParallelThroughputLogger<TimestampedQuery<Integer>>(1000, configString + ";timestamped"));


        // currently this specific queryFunction is quite the abberation -> if possible refactor to use the signature we use above
        QueryFunction<TimestampedQuery<Integer>, WindowedSynopsis<CountMinSketch>, QueryResult<TimestampedQuery<Integer>, Integer>> queryFunction = new QueryFunction<TimestampedQuery<Integer>, WindowedSynopsis<CountMinSketch>, QueryResult<TimestampedQuery<Integer>, Integer>>() {
            @Override
            public QueryResult<TimestampedQuery<Integer>, Integer> query(TimestampedQuery<Integer> query, WindowedSynopsis<CountMinSketch> synopsis) {
                return new QueryResult<TimestampedQuery<Integer>, Integer>(synopsis.getSynopsis().query(query.getQuery()), query, synopsis);
            }
        };

        final SingleOutputStreamOperator<QueryResult<TimestampedQuery<Integer>, Integer>> queryResults =
                ApproximateDataAnalytics.queryTimestamped(synopsesStream, timestampedQueries, queryFunction, 120);

        if (outputDir == null){
            queryResults.addSink(new SinkFunction<QueryResult<TimestampedQuery<Integer>, Integer>>() {
                @Override
                public void invoke(QueryResult<TimestampedQuery<Integer>, Integer> value, Context context) throws Exception {
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

    static JobExecutionResult runQueryStratifiedTimestamped(String configString, String outputDir, StreamExecutionEnvironment env, int sketchThroughput, int queryThroughput, int stratification, BuildSynopsisConfig config, Object... params){

        final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> sourceStream = env.addSource(new IP_Address_Source(20000, sketchThroughput))
                .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());

        SingleOutputStreamOperator<StratifiedSynopsisWrapper<Integer, WindowedSynopsis<CountMinSketch>>> stratifiedSynopsisStream = BuildStratifiedSynopsis
                .timeBasedADA(sourceStream, config.getWindowTime(), config.getSlideTime(), new Stratifier(stratification), CountMinSketch.class, params);

        DataStream<Tuple2<Integer, TimestampedQuery<Integer>>> queryStream = env.addSource(new StratifiedTimestampedQuerySource(queryThroughput, Time.seconds(40), Time.seconds(20), Time.seconds(20), stratification))
                .flatMap(new ParallelThroughputLogger<Tuple2<Integer, TimestampedQuery<Integer>>>(1000, configString + ";stratified_timestamped"));

        QueryFunction<Tuple2<Integer, TimestampedQuery<Integer>>, WindowedSynopsis<CountMinSketch>, StratifiedQueryResult<TimestampedQuery<Integer>, Integer, Integer>> queryFunction = new QueryFunction<Tuple2<Integer, TimestampedQuery<Integer>>, WindowedSynopsis<CountMinSketch>, StratifiedQueryResult<TimestampedQuery<Integer>, Integer, Integer>>() {
            @Override
            public StratifiedQueryResult<TimestampedQuery<Integer>, Integer, Integer> query(Tuple2<Integer, TimestampedQuery<Integer>> query, WindowedSynopsis<CountMinSketch> synopsis) {
                Integer result = synopsis.getSynopsis().query(query.f1.getQuery());
                return new StratifiedQueryResult<TimestampedQuery<Integer>, Integer, Integer>(result, query, synopsis);
            }
        };

        final SingleOutputStreamOperator<StratifiedQueryResult<TimestampedQuery<Integer>, Integer, Integer>> queryResultStream =
                ApproximateDataAnalytics.queryTimestampedStratified(stratifiedSynopsisStream, queryStream, queryFunction, Integer.class, 100);

        if (outputDir == null){
            queryResultStream.addSink(new SinkFunction<StratifiedQueryResult<TimestampedQuery<Integer>, Integer, Integer>>() {
                @Override
                public void invoke(StratifiedQueryResult<TimestampedQuery<Integer>, Integer, Integer> value, Context context) throws Exception {
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
