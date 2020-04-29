package StreamApprox;

import Benchmark.FlinkBenchmarkJobs.NormalFlinkJob;
import Benchmark.ParallelThroughputLogger;
import Benchmark.Sources.NormalDistributionSource;
import Benchmark.Sources.UniformDistributionSource;
import Benchmark.Sources.ZipfDistributionSource;
import FlinkScottyConnector.BuildStratifiedSynopsis;
import Synopsis.Sampling.ReservoirSampler;
import akka.stream.scaladsl.Zip;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.LinkedList;
import java.util.List;

public class FlinkBenchmarkJob {
    public FlinkBenchmarkJob(ApproxConfiguration config, StreamExecutionEnvironment env, String configString){
        List<Tuple2<Long, Long>> gaps = new LinkedList<>();


        DataStreamSource<Tuple3<Integer, Integer, Long>> messageStream = null;
        if (config.source == Source.Zipf){
            messageStream = env.addSource(new ZipfDistributionSource(config.runtime, config.throughput, gaps));
        }else if (config.source == Source.Uniform){
            messageStream = env.addSource(new UniformDistributionSource(config.runtime, config.throughput, gaps));
        }

        messageStream.flatMap(new ParallelThroughputLogger<Tuple3<Integer, Integer, Long>>(1000, configString));

        final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
                .assignTimestampsAndWatermarks(new NormalFlinkJob.TimestampsAndWatermarks());

        SingleOutputStreamOperator<ReservoirSampler> synopsisStream = BuildStratifiedSynopsis.timeBased(timestamped, Time.seconds(6), Time.seconds(3), new RichStratifier(), ReservoirSampler.class, config.sampleSize);

        synopsisStream.addSink(new SinkFunction<ReservoirSampler>() {
            @Override
            public void invoke(ReservoirSampler value, Context context) throws Exception {

            }
        });

        try {
            env.execute("Flink Reservoir Sampling Benchmark Job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
