package Benchmark.ScottyBenchmarkJobs;

import Benchmark.ParallelThroughputLogger;
import Benchmark.RichStratifier;
import Benchmark.RichStratifierNYC;
import Benchmark.Sources.NYCTaxiRideSource;
import FlinkScottyConnector.BuildStratifiedSynopsis;
import FlinkScottyConnector.BuildSynopsis;
import Synopsis.MergeableSynopsis;
import de.tub.dima.scotty.core.AggregateWindow;
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

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

/**
 * Created by philipp on 5/28/17.
 */
public class NYCScottyJob<S extends MergeableSynopsis> {

	public NYCScottyJob(String configuration, List<Window> assigner, StreamExecutionEnvironment env, final long runtime,
						final int throughput, final List<Tuple2<Long, Long>> gaps, Class<S> synopsisClass, boolean stratified, Object[] parameters) {

		Map<String, String> configMap = new HashMap<>();
		ParameterTool parametersTool = ParameterTool.fromMap(configMap);

		env.getConfig().setGlobalJobParameters(parametersTool);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		Window[] windows = new Window[assigner.size()];
		for (int i = 0; i < assigner.size(); i++) {
			windows[i] = assigner.get(i);
		}

//		DataStream<Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short>> messageStream = env
//				.addSource(new NYCTaxiRideSource("C:\\Users\\Rudi\\Documents\\EDADS\\flink-training-exercises\\data\\nycTaxiRides.gz",runtime, throughput,  gaps));

		DataStream<Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short>> messageStream = env
				.addSource(new NYCTaxiRideSource(runtime, throughput,  gaps));

		final SingleOutputStreamOperator<Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short>> timestamped = messageStream
				.assignTimestampsAndWatermarks(new TimestampsAndWatermarks()).flatMap(new ParallelThroughputLogger<Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short>>(1000, configuration));

		SingleOutputStreamOperator<AggregateWindow<S>> synopsesStream;
		if (stratified){
//			synopsesStream = BuildStratifiedSynopsis.scottyWindows(timestamped, windows, 0, 0, synopsisClass, parameters);
			synopsesStream = BuildStratifiedSynopsis.scottyWindows(timestamped,windows,new RichStratifierNYC(env.getParallelism()),synopsisClass, parameters);
		} else {
			synopsesStream = BuildSynopsis.scottyWindows(timestamped, windows, 0, synopsisClass, parameters);
		}
		synopsesStream.addSink(new SinkFunction() {

			@Override
			public void invoke(final Object value) throws Exception {
				//Environment.out.println(value);
			}
		});


//		synopsesStream.flatMap(new FlatMapFunction<AggregateWindow<S>, String>() {
//			@Override
//			public void flatMap(AggregateWindow<CountMinSketch> value, Collector<String> out) throws Exception {
//				String result = value.getStart()+" ---> "+value.getEnd()+"\n\n"+value.getAggValues().get(0).toString();
//				out.collect(result);
//			}
//		}).print();

//		finalSketch.writeAsText("EDADS/output/BenchmarkTest.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);


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
