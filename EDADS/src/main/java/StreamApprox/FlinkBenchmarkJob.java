package StreamApprox;

import Benchmark.FlinkBenchmarkJobs.NormalFlinkJob;
import Benchmark.ParallelThroughputLogger;
import Benchmark.Sources.NormalDistributionSource;
import FlinkScottyConnector.BuildStratifiedSynopsis;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.LinkedList;
import java.util.List;

public class FlinkBenchmarkJob {
    public FlinkBenchmarkJob(ApproxConfiguration config, StreamExecutionEnvironment env, String configString){
        List<Tuple2<Long, Long>> gaps = new LinkedList<>();

        DataStreamSource<Tuple3<Integer, Integer, Long>> messageStream = env.addSource(new NormalDistributionSource(config.runtime, config.throughput, gaps));

        messageStream.flatMap(new ParallelThroughputLogger<Tuple3<Integer, Integer, Long>>(1000, configString));

        final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
                .assignTimestampsAndWatermarks(new NormalFlinkJob.TimestampsAndWatermarks());

        BuildStratifiedSynopsis.slidingTimeBased(timestamped, Time.seconds(6), Time.seconds(3), 0, )
    }
}
