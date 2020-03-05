package StreamApprox;

import Benchmark.FlinkBenchmarkJobs.NormalFlinkJob;
import Benchmark.ParallelThroughputLogger;
import Benchmark.Sources.UniformDistributionSource;
import Benchmark.Sources.ZipfDistributionSource;
import FlinkScottyConnector.BuildStratifiedSynopsis;
import Synopsis.Sampling.ReservoirSampler;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.LinkedList;
import java.util.List;

public class ScottyBenchmarkJob {
    public ScottyBenchmarkJob(ApproxConfiguration config, StreamExecutionEnvironment env, String configString){
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

        Window window = new SlidingWindow(WindowMeasure.Time, 6000, 3000); // creates a window of of size 6 seconds which slides every 3 seconds
        Window[] windows = {window};


        SingleOutputStreamOperator<AggregateWindow<ReservoirSampler>> scottyWindows = BuildStratifiedSynopsis.scottyWindows(timestamped, windows, new RichStratifier(), ReservoirSampler.class, config.sampleSize);

        scottyWindows.addSink(new SinkFunction<AggregateWindow<ReservoirSampler>>() {
            @Override
            public void invoke(AggregateWindow<ReservoirSampler> value, Context context) throws Exception {

            }
        });

        try {
            env.execute("Scotty Reservoir Sampling Benchmark Job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
