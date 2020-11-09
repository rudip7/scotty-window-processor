package Benchmark.AccuracyBenchmark;

import Benchmark.FlinkBenchmarkJobs.NYCFlinkJob;
import Benchmark.Sources.NYCTaxiRideSource;
import Benchmark.Sources.NYCTaxiRideSourceDoubles;
import FlinkScottyConnector.BuildSynopsis;
import Synopsis.Histograms.EquiWidthHistogram;
import Synopsis.MergeableSynopsis;
import Synopsis.Sketches.CountMinSketch;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.ArrayList;

import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

/**
 * Created by Rudi on 22/10/2020.
 */
public class EquiWidthHistogramAccuracy {
    public static void main(String[] args) throws Exception {

        System.out.println("Equi-width histogram accuracy test");
//		double bL = (-73.991119 - (-73.965118)) / (double) 100;
//		System.out.println(bL);

        // set up the streaming execution Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		env.setParallelism(Integer.parseInt(args[0]));
		env.setMaxParallelism(Integer.parseInt(args[0]));

//        env.setParallelism(1);
//        env.setMaxParallelism(1);

        Class<EquiWidthHistogram> synopsisClass = EquiWidthHistogram.class;

        DataStream<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>> messageStream = env
                .addSource(new NYCTaxiRideSourceDoubles(-1, 200000, new ArrayList<>())).setParallelism(1);


        final SingleOutputStreamOperator<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>> timestamped = messageStream
                .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());

//        SingleOutputStreamOperator<String> result = timestamped.flatMap(new Test(-73.991119, -73.965118, 100));

		SingleOutputStreamOperator<EquiWidthHistogram> synopsesStream = BuildSynopsis.timeBased(timestamped, Time.milliseconds(10000),6, synopsisClass, new Object[]{-73.991119, -73.965118, 100});

		SingleOutputStreamOperator<Integer> result = synopsesStream.flatMap(new queryBucketCounts());

		result.writeAsText("/share/hadoop/EDADS/accuracyResults/ew-histogram_result_"+Integer.parseInt(args[0])+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

//        result.writeAsText("EDADS/output/indexes.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Equi-width histogram accuracy test");
    }

    private static class Test implements FlatMapFunction<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>, String> {
        double lowerBound, upperBound;
        int numBuckets;
        double bucketLength;

        public Test(Double lowerBound, Double upperBound, Integer numBuckets) {
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.numBuckets = numBuckets;
            this.bucketLength = (upperBound - lowerBound) / (double) numBuckets;
        }

        @Override
        public void flatMap(Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short> tuple, Collector<String> out) throws Exception {
            double input = tuple.f6.doubleValue();

            if (input >= upperBound || input < lowerBound) {
//            throw new IllegalArgumentException("input is out of Bounds!");

            } else {
                int index = (int) ((input - lowerBound) / bucketLength);

                out.collect(""+input+","+index);
            }
        }
    }

    private static class queryBucketCounts implements FlatMapFunction<EquiWidthHistogram, Integer> {

        @Override
        public void flatMap(EquiWidthHistogram histogram, Collector<Integer> out) throws Exception {
            //estimate the frequencies of all taxiID's [2013000001, 2013013223]
            int[] counts = histogram.getFrequency();
            for (int i = 0; i < counts.length; i++) {
                out.collect(counts[i]);
            }
        }
    }

    public static class TimestampsAndWatermarks implements AssignerWithPeriodicWatermarks<Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short>> {
        private final long maxOutOfOrderness = seconds(20).toMilliseconds(); // 5 seconds
        private long currentMaxTimestamp;
        private long startTime = System.currentTimeMillis();

        @Override
        public long extractTimestamp(final Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short> element, final long previousElementTimestamp) {
            long timestamp = getEventTime(element);
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp);
        }

        public long getEventTime(Tuple11<Long, Long, Long, Boolean, Long, Long, Double, Double, Double, Double, Short> ride) {
            if (ride.f3) {
                return ride.f4;
            } else {
                return ride.f5;
            }
        }
    }


}
