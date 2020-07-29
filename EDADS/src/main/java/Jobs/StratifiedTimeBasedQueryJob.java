package Jobs;

import Benchmark.Sources.UniformDistributionSource;
import FlinkScottyConnector.BuildStratifiedSynopsis;
import FlinkScottyConnector.BuildSynopsisConfig;
import Synopsis.Sketches.DDSketch;
import Synopsis.WindowedSynopsis;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class StratifiedTimeBasedQueryJob {

    public static void main(String[] args){
        // Arguments & setup
        final int runtime = 10000; // runtime in milliseconds
        final int throughput = 100; // target throughput
        final List<Tuple2<Long, Long>> gaps = new ArrayList<>();
        final double accuracy = 0.01; // relative accuracy of DD-Sketch
        final int maxNumberOfBins = 2000; // maximum number of bins of DD-Sketch
//        final String pathToZipfData = "/Users/joschavonhein/Data/zipfTimestamped.gz";
        Object[] params = new Object[]{accuracy, maxNumberOfBins};
        BuildSynopsisConfig config = new BuildSynopsisConfig(Time.seconds(6L), Time.seconds(3L), 0);

        int stratification = 4;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // DataStreamSource<Tuple3<Integer, Integer, Long>> messageStream = env.addSource(new ZipfDistributionSource(pathToZipfData, runtime, throughput, gaps));
        DataStreamSource<Tuple3<Integer, Integer, Long>> messageStream = env.addSource(new UniformDistributionSource(runtime, throughput, gaps));

        final SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> timestamped = messageStream
                .assignTimestampsAndWatermarks(new ExampleStratifiedADAJob.TimestampsAndWatermarks());

        SingleOutputStreamOperator<WindowedSynopsis<DDSketch>> stratifiedSynopsesStream = BuildStratifiedSynopsis
                .timeBasedADA(timestamped, Time.seconds(6L), Time.seconds(3L), new Stratifier(stratification), DDSketch.class, params);



    }


    public static class Stratifier extends RichMapFunction<Tuple3<Integer, Integer, Long>, Tuple2<Integer, Integer>> {

        private int stratification;

        public Stratifier(int stratification) {
            this.stratification = stratification;
        }

        @Override
        public Tuple2<Integer, Integer> map(Tuple3<Integer, Integer, Long> value) throws Exception {
            // creates a key value on which to stratify based on the first value in the tuple. - basically creating a new vlaue to keyBy...

            int key = (int)(value.f0 / 100d * stratification);
            if (key >= stratification){
                key = stratification -1;
            }
            return new Tuple2<>(key, value.f0);
        }
    }
}
