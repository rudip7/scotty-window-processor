package Benchmark.FlinkBenchmarkJobs;

import Benchmark.ParallelThroughputLogger;
import Benchmark.Sources.NYCTaxiRideSource;
import Benchmark.Sources.NormalDistributionSource;
import Benchmark.Sources.UniformDistributionSource;
import Benchmark.Sources.ZipfDistributionSource;
import FlinkScottyConnector.BuildStratifiedSynopsis;
import FlinkScottyConnector.BuildSynopsis;
import Synopsis.MergeableSynopsis;
import Synopsis.Sampling.TimestampedElement;
import Synopsis.Synopsis;
import Synopsis.Wavelets.DistributedWaveletsManager;
import Synopsis.Wavelets.WaveletSynopsis;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.api.windowing.time.Time.seconds;


public class WaveletFlinkJob {

    public WaveletFlinkJob(String configuration, List<Window> assigners, StreamExecutionEnvironment env, final long runtime,
                           final int throughput, final List<Tuple2<Long, Long>> gaps, String source, boolean stratified) {


        Map<String, String> configMap = new HashMap<>();
        ParameterTool parametersTool = ParameterTool.fromMap(configMap);

        env.getConfig().setGlobalJobParameters(parametersTool);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        BuildStratifiedSynopsis.setParallelismKeys(env.getParallelism());
        BuildSynopsis.setParallelismKeys(env.getParallelism());

        if (source.contentEquals("NYC-taxi")) {
            DataStream<Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short>> messageStream = env
                    .addSource(new NYCTaxiRideSource(runtime, throughput, gaps));

//		messageStream.flatMap(new ThroughputLogger<>(throughput)).setParallelism(1);
            messageStream.flatMap(new ParallelThroughputLogger<Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short>>(1000, configuration));

            final SingleOutputStreamOperator<Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short>> timestamped = messageStream
                    .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());


            if (assigners.size() == 1) {
                SingleOutputStreamOperator<? extends Synopsis> synopsesStream;
                if (assigners.get(0) instanceof TumblingWindow) {
                    if (stratified) {
                        synopsesStream = BuildStratifiedSynopsis.timeBased(timestamped, Time.milliseconds(((TumblingWindow) assigners.get(0)).getSize()), null, new RichStratifierNYC(env.getParallelism()), WaveletSynopsis.class,  10000);
                    } else {
                        synopsesStream = BuildSynopsis.timeBased(timestamped, env.getParallelism() * 2, Time.milliseconds(((TumblingWindow) assigners.get(0)).getSize()), null, 0, WaveletSynopsis.class, DistributedWaveletsManager.class, 10000);
                    }
                } else if (assigners.get(0) instanceof SlidingWindow) {
                    if (stratified) {
                        synopsesStream = BuildStratifiedSynopsis.timeBased(timestamped, Time.milliseconds(((SlidingWindow) assigners.get(0)).getSize()), Time.milliseconds(((SlidingWindow) assigners.get(0)).getSlide()), new RichStratifierNYC(env.getParallelism()), WaveletSynopsis.class,  10000);
                    } else {
                        synopsesStream = BuildSynopsis.timeBased(timestamped, env.getParallelism() * 2, Time.milliseconds(((SlidingWindow) assigners.get(0)).getSize()), Time.milliseconds(((SlidingWindow) assigners.get(0)).getSlide()), 0, WaveletSynopsis.class, DistributedWaveletsManager.class, 10000);
                    }
                } else {
                    throw new IllegalArgumentException("Window not supported in benchmark.");
                }

                synopsesStream.addSink(new SinkFunction() {

                    @Override
                    public void invoke(final Object value) throws Exception {
                        //System.out.println(value);
                    }
                });

//			synopsesStream.flatMap(new FlatMapFunction<S, String>() {
//				@Override
//				public void flatMap(S value, Collector<String> out) throws Exception {
//					String result = value.toString()+"\n";
//					out.collect(result);
//				}
//			}).writeAsText("EDADS/output/rudiTest.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

            } else {
                throw new IllegalArgumentException("The Flink implementation supports only a single window definition.");
            }

        } else {
            DataStream<Tuple3<Integer, Integer, Long>> messageStream;
            if (source.contentEquals("Uniform")) {
                messageStream = env.addSource(new UniformDistributionSource(runtime, throughput, gaps));
            } else if (source.contentEquals("Normal")) {
                messageStream = env.addSource(new NormalDistributionSource(runtime, throughput, gaps));
            } else if (source.contentEquals("Zipf")) {
                messageStream = env.addSource(new ZipfDistributionSource(runtime, throughput, gaps));
            } else {
                throw new IllegalArgumentException("Source not supported.");
            }
            messageStream.flatMap(new ParallelThroughputLogger<Tuple3<Integer, Integer, Long>>(1000,configuration));

            final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
                    .assignTimestampsAndWatermarks(new TimestampsAndWatermarksTuple3());


            if (assigners.size() == 1) {
                SingleOutputStreamOperator<? extends Synopsis> synopsesStream;
                if (assigners.get(0) instanceof TumblingWindow) {
                    if (stratified) {
                        synopsesStream = BuildStratifiedSynopsis.timeBased(timestamped, Time.milliseconds(((TumblingWindow) assigners.get(0)).getSize()), null, new RichStratifier(env.getParallelism()), WaveletSynopsis.class,  10000);
                    } else {
                        synopsesStream = BuildSynopsis.timeBased(timestamped, env.getParallelism() * 2, Time.milliseconds(((TumblingWindow) assigners.get(0)).getSize()), null, 0, WaveletSynopsis.class, DistributedWaveletsManager.class, 10000);
                    }
                } else if (assigners.get(0) instanceof SlidingWindow) {
                    if (stratified) {
                        synopsesStream = BuildStratifiedSynopsis.timeBased(timestamped, Time.milliseconds(((SlidingWindow) assigners.get(0)).getSize()), Time.milliseconds(((SlidingWindow) assigners.get(0)).getSlide()),new RichStratifier(env.getParallelism()), WaveletSynopsis.class,  10000);
                    } else {
                        synopsesStream = BuildSynopsis.timeBased(timestamped, env.getParallelism() * 2, Time.milliseconds(((SlidingWindow) assigners.get(0)).getSize()), Time.milliseconds(((SlidingWindow) assigners.get(0)).getSlide()), 0, WaveletSynopsis.class, DistributedWaveletsManager.class, 10000);
                    }
                } else {
                    throw new IllegalArgumentException("Window not supported in benchmark.");
                }

                synopsesStream.addSink(new SinkFunction() {

                    @Override
                    public void invoke(final Object value) throws Exception {
//                        System.out.println(value);
                    }
                });

//			synopsesStream.flatMap(new FlatMapFunction<S, String>() {
//				@Override
//				public void flatMap(S value, Collector<String> out) throws Exception {
//					String result = value.toString()+"\n";
//					out.collect(result);
//				}
//			}).writeAsText("EDADS/output/rudiTest.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

            } else {
                throw new IllegalArgumentException("The Flink implementation supports only a single window definition.");
            }

        }

        try {
            env.execute(configuration);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static class RichStratifier extends RichMapFunction<Tuple3<Integer, Integer, Long>, Tuple2<Integer, Integer>> {

        private int stratification;

        public RichStratifier(int stratification) {
            this.stratification = stratification;
        }

        @Override
        public Tuple2<Integer, Integer> map(Tuple3<Integer, Integer, Long> value) throws Exception {
            int key = (int) (value.f0 / 100d * stratification);
            if (key >= stratification) {
                key = stratification - 1;
            }
            return new Tuple2<>(key, value.f0);
        }
    }

    public static class RichStratifierNYC extends RichMapFunction<Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short>, Tuple2<Integer, Long>> {

        private int stratification;

        public RichStratifierNYC(int stratification) {
            this.stratification = stratification;
        }

        @Override
        public Tuple2<Integer, Long> map(Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short> value) throws Exception {
            int key = (int) (value.f0 / 100d * stratification);
            if (key >= stratification) {
                key = stratification - 1;
            }
            return new Tuple2<>(key, value.f0);
        }
    }


    public static class TimestampsAndWatermarks implements AssignerWithPeriodicWatermarks<Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short>> {
        private final long maxOutOfOrderness = seconds(20).toMilliseconds(); // 5 seconds
        private long currentMaxTimestamp;
        private long startTime = System.currentTimeMillis();

        @Override
        public long extractTimestamp(final Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short> element, final long previousElementTimestamp) {
            long timestamp = getEventTime(element);
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp);
        }

        public long getEventTime(Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short> ride) {
            if (ride.f3) {
                return ride.f4;
            } else {
                return ride.f5;
            }
        }
    }


    public static class TimestampsAndWatermarksTuple3 implements AssignerWithPeriodicWatermarks<Tuple3<Integer, Integer, Long>> {
        private final long maxOutOfOrderness = seconds(20).toMilliseconds(); // 5 seconds
        private long currentMaxTimestamp;
        private long startTime = System.currentTimeMillis();

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
}
