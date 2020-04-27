package Jobs;

import Synopsis.Sampling.ReservoirSampler;
import Synopsis.Sketches.CountMinSketch;
import Source.DemoSource;
import FlinkScottyConnector.BuildSynopsis;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

public class BuildSynopsisJob {
    public static void main(String[] args) throws Exception {


        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Tuple3<Integer, Integer, Long>> timestamped = env.addSource(new DemoSource(10));

        Window[] windows = {new SlidingWindow(WindowMeasure.Time, 5000, 1000)};

//        SingleOutputStreamOperator<AggregateWindow<ReservoirSampler>> finalSketch = BuildSynopsis.scottyStratifiedSampling(timestamped, windows, 0, ReservoirSampler.class, 10);
        SingleOutputStreamOperator<AggregateWindow<CountMinSketch>> finalSketch = BuildSynopsis.scottyWindows(timestamped, windows, 0, CountMinSketch.class, 10, 10, 1L);

        //        SingleOutputStreamOperator<AggregateWindow<ReservoirSampler>> finalSketch = BuildSynopsis.scottyWindowsRescale(timestamped, windows, 0, ReservoirSampler.class, 10);

//        SingleOutputStreamOperator<AggregateWindow<CountMinSketch>> finalSketch = BuildSynopsis.scottyWindowsRescale(timestamped, windows, 0, CountMinSketch.class, 10, 10, 1L);
//        BuildSynopsis.setParallelismKeys(env.getParallelism()*2);
//        SingleOutputStreamOperator<AggregateWindow<BloomFilter>> finalSketch = BuildSynopsis.scottyWindowsRescale(timestamped, windows, 0, BloomFilter.class, 25, 20, 1L);
//        SingleOutputStreamOperator<AggregateWindow<BiasedReservoirSampler>> finalSketch = BuildSynopsis.scottyWindowsRescale(timestamped, windows, 0, BiasedReservoirSampler.class, 20);

//        KeyedStream<Tuple2<Integer, Tuple3<Integer, Integer, Long>>, Tuple> keyedStream = timestamped.map(new BuildSynopsis.AddParallelismIndex<>()).keyBy(0);
//        KeyedScottyWindowOperator windowOperator = new KeyedScottyWindowOperator<>(new InvertibleSynopsisFunction(0, CountMinSketch.class, 10, 10, 1L));
//
//        windowOperator.addWindow(new TumblingWindow(WindowMeasure.Time, 5000));
//
//        SingleOutputStreamOperator<AggregateWindow<CountMinSketch>> finalSketch = keyedStream
//                .process(windowOperator);

        finalSketch
                .flatMap(new FlatMapFunction<AggregateWindow<CountMinSketch>, String>() {
                    @Override
                    public void flatMap(AggregateWindow<CountMinSketch> value, Collector<String> out) throws Exception {
                        String result = value.getStart()+" ---> "+value.getEnd()+"\n"+value.getAggValues().get(0).toString()+"\n";//+value.getAggValues().get(0).toString();
                        out.collect(result);

//                out.collect();
//                for (CountMinSketch w: value.getAggValues()){
//                    out.collect(w.toString());
//                }
                    }
                })
//                .print();

                .writeAsText("EDADS/output/scottyTest.txt", FileSystem.WriteMode.OVERWRITE);

        env.execute("Flink Streaming Java API Skeleton");
    }



    /**
     * FlatMap to create Tuples from the incoming data
     */
    static class CreateTuplesFlatMap implements FlatMapFunction<String, Tuple3<Integer, Integer, Long>> {
        @Override
        public void flatMap(String value, Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {
            String[] tuples = value.split(",");

            if(tuples.length == 3) {

                Integer key = new Integer(tuples[0]);
                Integer val = new Integer(tuples[1]);
                Long timestamp = new Long(tuples[2]);

                if (key != null && val != null) {
                    out.collect(new Tuple3<>(key, val, timestamp));
                }
            }
        }
    }

    /**
     * The Custom TimeStampExtractor which is used to assign Timestamps and Watermarks for our data
     */
    public static class CustomTimeStampExtractor implements AssignerWithPunctuatedWatermarks<Tuple3<Integer, Integer, Long>> {
        /**
         * Asks this implementation if it wants to emit a watermark. This method is called right after
         * the {@link #extractTimestamp(Tuple3, long)}   method.
         *
         * <p>The returned watermark will be emitted only if it is non-null and its timestamp
         * is larger than that of the previously emitted watermark (to preserve the contract of
         * ascending watermarks). If a null value is returned, or the timestamp of the returned
         * watermark is smaller than that of the last emitted one, then no new watermark will
         * be generated.
         *
         * <p>For an example how to use this method, see the documentation of
         * {@link AssignerWithPunctuatedWatermarks this class}.
         *
         * @param lastElement
         * @param extractedTimestamp
         * @return {@code Null}, if no watermark should be emitted, or the next watermark to emit.
         */
        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Tuple3<Integer, Integer, Long> lastElement, long extractedTimestamp) {
            return new Watermark(extractedTimestamp-1000);
        }

        /**
         * Assigns a timestamp to an element, in milliseconds since the Epoch.
         *
         * <p>The method is passed the previously assigned timestamp of the element.
         * That previous timestamp may have been assigned from a previous assigner,
         * by ingestion time. If the element did not carry a timestamp before, this value is
         * {@code Long.MIN_VALUE}.
         *
         * @param element                  The element that the timestamp will be assigned to.
         * @param previousElementTimestamp The previous internal timestamp of the element,
         *                                 or a negative value, if no timestamp has been assigned yet.
         * @return The new timestamp.
         */
        @Override
        public long extractTimestamp(Tuple3<Integer, Integer, Long> element, long previousElementTimestamp) {
            return element.f2;
        }
    }
}