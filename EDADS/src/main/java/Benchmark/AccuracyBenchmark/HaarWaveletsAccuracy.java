package Benchmark.AccuracyBenchmark;

import Benchmark.FlinkBenchmarkJobs.NYCFlinkJob;
import Benchmark.Sources.NYCTaxiRideSource;
import FlinkScottyConnector.BuildSynopsis;
import Synopsis.Sampling.ReservoirSampler;
import Synopsis.Wavelets.DistributedWaveletsManager;
import Synopsis.Wavelets.WaveletSynopsis;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * Created by Rudi on 22/10/2020.
 */
public class HaarWaveletsAccuracy {
	public static void main(String[] args) throws Exception {

		System.out.println("Haar Wavelets accuracy test");
		// set up the streaming execution Environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		env.setParallelism(Integer.parseInt(args[0]));
		env.setMaxParallelism(Integer.parseInt(args[0]));

		BuildSynopsis.setParallelismKeys(Integer.parseInt(args[0]));
//
//		env.setParallelism(1);
//		env.setMaxParallelism(1);

		int waveletSize = 10000;
		if (Integer.parseInt(args[0]) == 1){
			waveletSize = 1000;
		} else if (Integer.parseInt(args[0]) == 256){
			waveletSize = 1000;
		}

		DataStream<Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short>> messageStream = env
				.addSource(new NYCTaxiRideSource(-1, 200000,  new ArrayList<>())).setParallelism(1);


		final SingleOutputStreamOperator<Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short>> timestamped = messageStream
				.assignTimestampsAndWatermarks(new NYCFlinkJob.TimestampsAndWatermarks());

//		SingleOutputStreamOperator<WaveletSynopsis> synopsesStream = BuildSynopsis.timeBased(timestamped, Time.milliseconds(10000),10, synopsisClass, new Object[]{10000});
		SingleOutputStreamOperator<DistributedWaveletsManager> synopsesStream = BuildSynopsis.timeBased(timestamped, env.getParallelism() * 10, Time.milliseconds(10000), null, 10, WaveletSynopsis.class, DistributedWaveletsManager.class, waveletSize);

		SingleOutputStreamOperator<Double> result = synopsesStream.flatMap(new rangeSumPassengerCount());
//        result.writeAsText("EDADS/output/avgPassengerCount.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		result.writeAsText("/share/hadoop/EDADS/accuracyResults/haar-wavelets_result_"+Integer.parseInt(args[0])+".csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

		env.execute("Haar Wavelets accuracy test");
	}

	private static class rangeSumPassengerCount implements FlatMapFunction<DistributedWaveletsManager, Double>{

		@Override
		public void flatMap(DistributedWaveletsManager waveletsManager, Collector<Double> out) throws Exception {
			//estimate the range sums of the passengers counts
			int rangeSize = 10000;
			for (int i = 0; i < 2999998; i+=rangeSize) {
				try {
					out.collect(waveletsManager.rangeSumQuery(i,i+rangeSize-1));
				} catch (IllegalArgumentException e){
					System.out.println("[ "+i+", "+(i+rangeSize-1)+" ]");
				}
			}
		}
	}


}
