package FlinkScottyConnector;

import Synopsis.NonMergeableSynopsisManager;
import FlinkScottyConnector.FunctionClasses.*;
import Synopsis.MergeableSynopsis;
import Synopsis.Sampling.SamplerWithTimestamps;
import Synopsis.Synopsis;
import Synopsis.WindowedSynopsis;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
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

    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> timeBased(DataStream<T> inputStream, TimeBasedConfig config){

        AllWindowedStream allWindowedStream = timeBasedHelperFunction(inputStream, config);

        SingleOutputStreamOperator result = allWindowedStream.reduce(new ReduceFunction<S>() { // Merge all sketches in the global window
            @Override
            public MergeableSynopsis reduce(MergeableSynopsis value1, MergeableSynopsis value2) throws Exception {
                MergeableSynopsis merged = value1.merge(value2);
                return merged;
            }
        }).returns(config.synopsisClass);
        return result;
    }

    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<WindowedSynopsis<S>> timeBasedWithWindowTimes(DataStream<T> inputStream, TimeBasedConfig config){
        if (config.getWindowTime() == null){
            throw new IllegalArgumentException("windowTime == null! must be set in order for this function to work!");
        }

        AllWindowedStream partialAggregate = timeBasedHelperFunction(inputStream, config);

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

    private static <T, S extends MergeableSynopsis> AllWindowedStream timeBasedHelperFunction(DataStream<T> inputStream, TimeBasedConfig config) {
        SynopsisAggregator agg = new SynopsisAggregator(config.synopsisClass, config.synParams);

        DataStream<T> rescaled = inputStream.rescale();
        KeyedStream keyBy;
        if (SamplerWithTimestamps.class.isAssignableFrom(config.synopsisClass)) {
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

    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> countBased(DataStream<T> inputStream, CountBasedConfig config) {
        SynopsisAggregator agg = new SynopsisAggregator(config.synopsisClass, config.synParams);
        int parallelism = inputStream.getExecutionEnvironment().getParallelism();

        KeyedStream keyBy;
        DataStream<T> rescaled = inputStream.rescale();
        if (SamplerWithTimestamps.class.isAssignableFrom(config.synopsisClass)) {
            keyBy = rescaled
                    .process(new ConvertToSample(config.keyField))
                    .assignTimestampsAndWatermarks(new SampleTimeStampExtractor())
                    .map(new AddParallelismIndex(-1))
                    .keyBy(0);
        } else {
            keyBy = rescaled
                    .map(new AddParallelismIndex(config.keyField))
                    .keyBy(0);
        }
        WindowedStream windowedStream;
        if (config.slideSize == -1) {
            windowedStream = keyBy.countWindow(config.windowSize / parallelism);
        } else {
            windowedStream = keyBy.countWindow(config.windowSize / parallelism, config.slideSize / parallelism);
        }

        SingleOutputStreamOperator preAggregated = windowedStream
                .aggregate(agg);

        SingleOutputStreamOperator result = preAggregated.flatMap(new MergeCountPreAggregates(getParallelismKeys())).returns(config.synopsisClass);
        return result;
    }

    public static <T, S extends Synopsis, M extends NonMergeableSynopsisManager> SingleOutputStreamOperator<M> nonMergeableTimeBased(DataStream<T> inputStream, NonMergeableTimeBasedConfig config) {
        NonMergeableSynopsisAggregator agg = new NonMergeableSynopsisAggregator(config.synopsisClass, config.synParams);
        KeyedStream keyBy = inputStream
                .process(new OrderAndIndex(config.keyField, config.miniBatchSize, getParallelismKeys())).setParallelism(1)
                .keyBy(0);

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

        SingleOutputStreamOperator returns = allWindowedStream.aggregate(new NonMergeableSynopsisUnifier(config.managerClass))
                .returns(config.managerClass);
        return returns;
    }

    public static <T, S extends Synopsis, M extends NonMergeableSynopsisManager> SingleOutputStreamOperator<M> countBased(DataStream<T> inputStream, NonMergeableCountBasedConfig config) {
        int parallelism = inputStream.getExecutionEnvironment().getParallelism();

        NonMergeableSynopsisAggregator agg = new NonMergeableSynopsisAggregator(config.synopsisClass, config.synParams);
        KeyedStream keyBy = inputStream
                .process(new OrderAndIndex(config.keyField, config.miniBatchSize, getParallelismKeys())).setParallelism(1)
                .keyBy(0);

        WindowedStream windowedStream;
        if (config.slideSize == -1) {
            windowedStream = keyBy.countWindow(config.windowSize / parallelism);
        } else {
            windowedStream = keyBy.countWindow(config.windowSize / parallelism, config.slideSize / parallelism);
        }

        SingleOutputStreamOperator preAggregated = windowedStream
                .aggregate(agg);

        SingleOutputStreamOperator returns = preAggregated.flatMap(new NonMergeableSynopsisCountUnifier(config.managerClass, getParallelismKeys()))
                .returns(config.managerClass);
        return returns;
    }











    /**
     * Stateful map functions to add the parallelism variable
     *
     * @param <T0> type of input elements
     */
    public static class AddParallelismIndex<T0> extends RichMapFunction<T0, Tuple2<Integer, Object>> {
        public int keyField;
        private Tuple2<Integer, Object> newTuple;


        public AddParallelismIndex(int keyField) {
            this.keyField = keyField;
            newTuple = new Tuple2<Integer, Object>();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            if (parallelismKeys < 1) {
                setParallelismKeys(this.getRuntimeContext().getNumberOfParallelSubtasks());
            }
        }

        @Override
        public Tuple2<Integer, Object> map(T0 value) throws Exception {
            if (value instanceof Tuple && keyField != -1) {
                newTuple.setField(((Tuple) value).getField(keyField), 1);
            } else {
                newTuple.setField(value, 1);
            }
            newTuple.setField(this.getRuntimeContext().getIndexOfThisSubtask(), 0);
            return newTuple;
        }
    }
}
