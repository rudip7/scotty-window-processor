package Benchmark;

import FlinkScottyConnector.BuildSynopsis;
import Synopsis.MergeableSynopsis;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
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

/**
 * Created by philipp on 5/28/17.
 */
public class FlinkBenchmarkJob<S extends MergeableSynopsis> {

	public FlinkBenchmarkJob(List<Window> assigners, StreamExecutionEnvironment env, final long runtime,
                             final int throughput, final List<Tuple2<Long, Long>> gaps, Class<S> synopsisClass, Object[] parameters) {


		Map<String, String> configMap = new HashMap<>();
		ParameterTool parametersTool = ParameterTool.fromMap(configMap);

		env.getConfig().setGlobalJobParameters(parametersTool);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Tuple3<Integer, Integer, Long>> messageStream = env
				.addSource(new LoadGeneratorSource(runtime, throughput,  gaps));

//		messageStream.flatMap(new ThroughputLogger<>(throughput)).setParallelism(1);
		messageStream.flatMap(new ParallelThroughputLogger<>(1000));

		final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
				.assignTimestampsAndWatermarks(new TimestampsAndWatermarks());


		for (Window w : assigners) {

			if (w instanceof TumblingWindow) {
				SingleOutputStreamOperator<S> synopsesStream = BuildSynopsis.timeBased(messageStream, Time.milliseconds(((TumblingWindow) w).getSize()), 0, synopsisClass, parameters);
				synopsesStream.addSink(new SinkFunction() {

					@Override
					public void invoke(final Object value) throws Exception {
						//System.out.println(value);
					}
				});
			}
			if (w instanceof SlidingWindow) {
				SingleOutputStreamOperator<S> synopsesStream = BuildSynopsis.slidingTimeBased(messageStream, Time.milliseconds(((SlidingWindow) w).getSize()),Time.milliseconds(((SlidingWindow) w).getSlide()), 0, synopsisClass, parameters);
				synopsesStream.addSink(new SinkFunction() {

					@Override
					public void invoke(final Object value) throws Exception {
						//System.out.println(value);
					}
				});
			}


		}

		try {
			env.execute();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}




	public static class TimestampsAndWatermarks implements AssignerWithPeriodicWatermarks<Tuple3<Integer, Integer, Long>> {
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
