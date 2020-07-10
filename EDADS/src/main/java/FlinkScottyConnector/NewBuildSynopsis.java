package FlinkScottyConnector;

import Synopsis.MergeableSynopsis;
import Synopsis.Sampling.SamplerWithTimestamps;
import Synopsis.WindowedSynopsis;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.function.Consumer;


/**
 * Class to organize the static functions to generate window based Synopses
 *
 * @author Joscha von Hein
 * @author Rudi Poepsel Lemaitre
 */
public class NewBuildSynopsis {

    private static int parallelismKeys = -1;

    public static void setParallelismKeys(int newParallelismKeys) {
        parallelismKeys = newParallelismKeys;
        System.out.println("BuildSynopsis Parallelism Keys changed to: "+parallelismKeys);
    }

    public static int getParallelismKeys(){
        return parallelismKeys;
    }

    /**
     * Build an operator pipeline to generate a stream of time window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#timeWindow} will accumulate the a MergeableSynopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single MergeableSynopsis representing the whole window.
     *
     * @param inputStream   the data stream to build the MergeableSynopsis
     * @param synopsisClass the type of MergeableSynopsis to be computed
     * @param config        additional optional parameters like window time, slide time and key-field
     * @param parameters    the initialization parameters for the MergeableSynopsis
     * @param <T>           the type of the input elements
     * @param <S>           the type of the Mergeable Synopsis
     * @return stream of the window based Synopis
     */
    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> timeBased(DataStream<T> inputStream, Class<S> synopsisClass, BuildSynopsisConfig config, Object... parameters){

        AllWindowedStream allWindowedStream = timeBasedHelperFunction(inputStream, synopsisClass, config, parameters);

        SingleOutputStreamOperator result = allWindowedStream.reduce(new ReduceFunction<S>() { // Merge all sketches in the global window
            @Override
            public MergeableSynopsis reduce(MergeableSynopsis value1, MergeableSynopsis value2) throws Exception {
                MergeableSynopsis merged = value1.merge(value2);
                return merged;
            }
        }).returns(synopsisClass);
        return result;
    }

    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<WindowedSynopsis<S>> timeBasedWithWindowTimes(DataStream<T> inputStream, Class<S> synopsisClass, BuildSynopsisConfig config, Object... parameters){
        if (config.getWindowTime() == null){
            throw new IllegalArgumentException("windowTime == null! must be set in order for this function to work!");
        }

        AllWindowedStream partialAggregate = timeBasedHelperFunction(inputStream, synopsisClass, config, parameters);

        return partialAggregate
                .reduce(new ReduceFunction<S>() {
                    public S reduce(S value1, S value2) throws Exception {
                        S merged = (S) value1.merge(value2);
                        return merged;
                    }
                }, new AllWindowFunction<S, WindowedSynopsis<S>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable values, Collector out) throws Exception {
                        values.forEach(new Consumer<S>() {
                            @Override
                            public void accept(S synopsis) {
                                out.collect(new WindowedSynopsis<S>(synopsis, window.getStart(), window.getEnd()));
                            }
                        });
                    }
                }).returns(WindowedSynopsis.class);
    }

    private static <T, S extends MergeableSynopsis> AllWindowedStream timeBasedHelperFunction(DataStream<T> inputStream, Class<S> synopsisClass, BuildSynopsisConfig config, Object... parameters) {
        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters);

        DataStream<T> rescaled = inputStream.rescale();
        KeyedStream keyBy;
        if (SamplerWithTimestamps.class.isAssignableFrom(synopsisClass)) {
            keyBy = rescaled
                    .process(new BuildSynopsis.ConvertToSample(config.keyField))
                    .assignTimestampsAndWatermarks(new BuildSynopsis.SampleTimeStampExtractor())
                    .map(new BuildSynopsis.AddParallelismIndex(-1))
                    .keyBy(0);
        } else {
            keyBy = rescaled
                    .map(new BuildSynopsis.AddParallelismIndex(config.keyField))
                    .keyBy(0);
        }

        WindowedStream windowedStream;
        if (config.slideTime == null) {
            windowedStream = keyBy.timeWindow(config.windowTime);
        } else {
            windowedStream = keyBy.timeWindow(config.windowTime, config.slideTime);
        }

        SingleOutputStreamOperator preAggregated = windowedStream
                .aggregate(agg);

        AllWindowedStream allWindowedStream;
        if (config.slideTime == null) {
            allWindowedStream = preAggregated.timeWindowAll(config.windowTime);
        } else {
            allWindowedStream = preAggregated.timeWindowAll(config.windowTime, config.slideTime);
        }

        return allWindowedStream;
    }

    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> countBased(DataStream<T> inputStream, long windowSize, long slideSize, int keyField, Class<S> synopsisClass, Object... parameters){
        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters);
        int parallelism = inputStream.getExecutionEnvironment().getParallelism();

        KeyedStream keyBy;
        DataStream<T> rescaled = inputStream.rescale();
        if (SamplerWithTimestamps.class.isAssignableFrom(synopsisClass)) {
            keyBy = rescaled
                    .process(new BuildSynopsis.ConvertToSample(keyField))
                    .assignTimestampsAndWatermarks(new BuildSynopsis.SampleTimeStampExtractor())
                    .map(new BuildSynopsis.AddParallelismIndex(-1))
                    .keyBy(0);
        } else {
            keyBy = rescaled
                    .map(new BuildSynopsis.AddParallelismIndex(keyField))
                    .keyBy(0);
        }
        WindowedStream windowedStream;
        if (slideSize == -1) {
            windowedStream = keyBy.countWindow(windowSize / parallelism);
        } else {
            windowedStream = keyBy.countWindow(windowSize / parallelism, slideSize / parallelism);
        }

        SingleOutputStreamOperator preAggregated = windowedStream
                .aggregate(agg);

        SingleOutputStreamOperator result = preAggregated.flatMap(new BuildSynopsis.MergeCountPreAggregates(getParallelismKeys())).returns(synopsisClass);
        return result;
    }
}
