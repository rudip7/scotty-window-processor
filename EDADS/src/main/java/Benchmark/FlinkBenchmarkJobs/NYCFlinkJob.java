package Benchmark.FlinkBenchmarkJobs;

import Benchmark.ParallelThroughputLogger;
import Benchmark.Sources.NYCTaxiRideSource;
import FlinkScottyConnector.BuildStratifiedSynopsis;
import FlinkScottyConnector.BuildSynopsis;
import Synopsis.MergeableSynopsis;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple2;
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

/**
 * Created by philipp on 5/28/17.
 */
public class NYCFlinkJob<S extends MergeableSynopsis> {

	public NYCFlinkJob(String configuration, List<Window> assigners, StreamExecutionEnvironment env, final long runtime,
                       final int throughput, final List<Tuple2<Long, Long>> gaps, Class<S> synopsisClass, boolean stratified, Object[] parameters) {


		Map<String, String> configMap = new HashMap<>();
		ParameterTool parametersTool = ParameterTool.fromMap(configMap);

		env.getConfig().setGlobalJobParameters(parametersTool);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short>> messageStream = env
				.addSource(new NYCTaxiRideSource(runtime, throughput,  gaps));

//		messageStream.flatMap(new ThroughputLogger<>(throughput)).setParallelism(1);
		messageStream.flatMap(new ParallelThroughputLogger<Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short>>(1000,configuration));

		final SingleOutputStreamOperator<Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short>> timestamped = messageStream
				.assignTimestampsAndWatermarks(new TimestampsAndWatermarks());


		if (assigners.size() == 1) {
			SingleOutputStreamOperator<S> synopsesStream;
			if (assigners.get(0) instanceof TumblingWindow) {
				if (stratified) {
					synopsesStream = BuildStratifiedSynopsis.timeBased(timestamped, Time.milliseconds(((TumblingWindow) assigners.get(0)).getSize()), 0, 0, synopsisClass, parameters);
				} else {
					synopsesStream = BuildSynopsis.timeBased(timestamped, Time.milliseconds(((TumblingWindow) assigners.get(0)).getSize()),0, synopsisClass, parameters);
				}
			} else if (assigners.get(0) instanceof SlidingWindow) {
				if (stratified){
					synopsesStream = BuildStratifiedSynopsis.timeBased(timestamped, Time.milliseconds(((SlidingWindow) assigners.get(0)).getSize()), Time.milliseconds(((SlidingWindow) assigners.get(0)).getSlide()), 0,0, synopsisClass, parameters);
				} else {
					synopsesStream = BuildSynopsis.timeBased(timestamped, Time.milliseconds(((SlidingWindow) assigners.get(0)).getSize()), Time.milliseconds(((SlidingWindow) assigners.get(0)).getSlide()), 0, synopsisClass, parameters);
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

		try {
			env.execute(configuration);

		} catch (Exception e) {
			e.printStackTrace();
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
}
