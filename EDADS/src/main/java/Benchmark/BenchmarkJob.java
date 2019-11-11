package Benchmark;

import FlinkScottyConnector.BuildSynopsis;
import Synopsis.Sampling.ReservoirSampler;
import Synopsis.Sketches.CountMinSketch;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.flinkconnector.KeyedScottyWindowOperator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

/**
 * Created by philipp on 5/28/17.
 */
public class BenchmarkJob {

	public BenchmarkJob(List<Window> assigner, StreamExecutionEnvironment env, final long runtime,
                        final int throughput, final List<Tuple2<Long, Long>> gaps) {


		Map<String, String> configMap = new HashMap<>();
		ParameterTool parameters = ParameterTool.fromMap(configMap);

		env.getConfig().setGlobalJobParameters(parameters);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//		Why no parallelism???
//		env.setParallelism(1);
//		env.setMaxParallelism(1);

		Window[] windows = new Window[assigner.size()];
		for (int i = 0; i < assigner.size(); i++) {
			windows[i] = assigner.get(i);
		}

		DataStream<Tuple3<Integer, Integer, Long>> messageStream = env
				.addSource(new LoadGeneratorSource(runtime, throughput,  gaps));

		SingleOutputStreamOperator<Integer> throughputStadistics = messageStream.flatMap(new ThroughputLogger<>(throughput)).setParallelism(1);

		final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
				.assignTimestampsAndWatermarks(new TimestampsAndWatermarks());

		SingleOutputStreamOperator<AggregateWindow<ReservoirSampler>> finalSketch = BuildSynopsis.scottyWindows(timestamped, windows, 0, ReservoirSampler.class, 10);


		//		finalSketch
//				.addSink(new SinkFunction() {
//
//					@Override
//					public void invoke(final Object value) throws Exception {
//						System.out.println(value);
//					}
//				});

//		finalSketch.flatMap(new FlatMapFunction<AggregateWindow<CountMinSketch>, String>() {
//			@Override
//			public void flatMap(AggregateWindow<CountMinSketch> value, Collector<String> out) throws Exception {
//				String result = value.getStart()+" ---> "+value.getEnd()+"\n\n"+value.getAggValues().get(0).toString();
//				out.collect(result);
//			}
//		}).print();

		finalSketch.writeAsText("EDADS/output/BenchmarkTest.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);


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
