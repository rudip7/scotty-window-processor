package Benchmark.AccuracyBenchmark;

import Benchmark.FlinkBenchmarkJobs.NYCFlinkJob;
import Benchmark.FlinkBenchmarkJobs.ZipfFlinkJob;
import Benchmark.ParallelThroughputLogger;
import Benchmark.Sources.NYCTaxiRideSource;
import Benchmark.Sources.UniformDistributionSource;
import Benchmark.Sources.ZipfDistributionSource;
import FlinkScottyConnector.BuildStratifiedSynopsis;
import FlinkScottyConnector.BuildSynopsis;
import Jobs.RudiTest;
import Synopsis.MergeableSynopsis;
import Synopsis.Sketches.CountMinSketch;
import Synopsis.Sketches.DDSketch;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.api.windowing.time.Time.seconds;

/**
 * Created by Rudi on 22/10/2020.
 */
public class CountMinAccuracy {
	public static void main(String[] args) throws Exception {

		System.out.println("Count-Min sketch accuracy test");
		// set up the streaming execution Environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		env.setParallelism(Integer.parseInt(args[0]));
		env.setMaxParallelism(Integer.parseInt(args[0]));

		Class<CountMinSketch> synopsisClass = CountMinSketch.class;

		DataStream<Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short>> messageStream = env
				.addSource(new NYCTaxiRideSource(-1, 200000,  new ArrayList<>())).setParallelism(1);


		final SingleOutputStreamOperator<Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short>> timestamped = messageStream
				.assignTimestampsAndWatermarks(new NYCFlinkJob.TimestampsAndWatermarks());

		SingleOutputStreamOperator<CountMinSketch> synopsesStream = BuildSynopsis.timeBased(timestamped, Time.milliseconds(10000),0, synopsisClass, new Object[]{633, 5, 7L});

		SingleOutputStreamOperator<Integer> result = synopsesStream.flatMap(new queryFrequency());

		result.writeAsText("/share/hadoop/EDADS/accuracyResults/count-min_result_"+Integer.parseInt(args[0])+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		env.execute("Count-Min sketch accuracy test");
	}

	private static class queryFrequency implements FlatMapFunction<CountMinSketch, Integer>{

		@Override
		public void flatMap(CountMinSketch cmSketch, Collector<Integer> out) throws Exception {
			//estimate the frequencies of all taxiID's [2013000001, 2013013223]
			for (int i = 2013000001; i <= 2013013223; i++) {
				out.collect(cmSketch.query(i));
			}
		}
	}


}
