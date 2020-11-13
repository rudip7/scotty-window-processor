package FlinkScottyConnector;

import FlinkScottyConnector.Configs.BuildConfiguration;
import FlinkScottyConnector.FunctionClasses.*;
import Synopsis.*;
import Synopsis.Sampling.SamplerWithTimestamps;
import Synopsis.Sampling.TimestampedElement;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowType.TumblingWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.flinkconnector.KeyedScottyWindowOperator;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.io.Serializable;
import java.util.function.Consumer;

import java.io.Serializable;

public class SynopsisBuilder {

    private static int parallelismKeys = -1;

    public static void setParallelismKeys(int newParallelismKeys) {
        parallelismKeys = newParallelismKeys;
        System.out.println("BuildSynopsis Parallelism Keys changed to: "+parallelismKeys);
    }

    public static int getParallelismKeys(){
        return parallelismKeys;
    }

    public static class AddParallelismIndex<T> extends RichMapFunction<T, Tuple2<Integer, T>> {

        @Override
        public void open(Configuration parameters) throws Exception {
            if (parallelismKeys < 1) {
                // TODO: check whether this is really necessary!!!
                setParallelismKeys(this.getRuntimeContext().getNumberOfParallelSubtasks());
            }
        }

        @Override
        public Tuple2<Integer, T> map(T value) throws Exception {

            return new Tuple2<>(this.getRuntimeContext().getIndexOfThisSubtask(), value);
        }
    }

    public static <T, S extends Synopsis> SingleOutputStreamOperator<WindowedSynopsis<S>> build(BuildConfiguration config) throws Exception {
        if(config.stratificationKeyExtractor == null){ // Stratified
            if(config.windows[0] instanceof TumblingWindow && config.windows.length == 1) {

                return buildFlink(config);

            } else {

                return buildScotty(config);

            }
        } else {
            throw new IllegalArgumentException("for stratified Methods use buildStratified() Method");
        }
    }

    public static <T, S extends Synopsis, Key extends Serializable, Value> SingleOutputStreamOperator<StratifiedSynopsisWrapper<Key, WindowedSynopsis<S>>> buildStratified(BuildConfiguration config) throws Exception {
        if(config.stratificationKeyExtractor != null){ // Stratified
            if(config.windows[0] instanceof TumblingWindow && config.windows.length == 1) {

                return buildFlinkStratified(config);

            } else {

                return buildScottyStratified(config);
            }
        }else{  // Not Stratified
            throw new IllegalArgumentException("Configuration needs a Stratification Key Extractor in order to build stratified synopsis");
        }
    }

    /**
     * @param <S>       The returned Synopsis Type, in the Non-Mergeable Case this will be a NonMergeableSynopsisManager
     */
    private static <S extends Synopsis, Key extends Serializable, Value> SingleOutputStreamOperator<StratifiedSynopsisWrapper<Key, WindowedSynopsis<S>>> buildFlinkStratified(BuildConfiguration config){


        KeyedStream<Tuple2<Key, Value>, Tuple> keyBy = config.inputStream
                .map(config.stratificationKeyExtractor)
                .keyBy(0);

        TumblingWindow window = (TumblingWindow) config.windows[0];
        WindowedStream windowedStream = window.getWindowMeasure() == WindowMeasure.Count ? keyBy.countWindow(window.getSize()) : keyBy.timeWindow(Time.milliseconds(window.getSize()));

        AggregateFunction aggregateFunction = MergeableSynopsis.class.isAssignableFrom(config.synopsisClass)
                ? new SynopsisAggregator(true, config.synopsisClass, config.synParams)
                : new NonMergeableSynopsisAggregator(true, config.synopsisClass, config.synParams);

        return windowedStream
                .aggregate(aggregateFunction, new WindowFunction<S, StratifiedSynopsisWrapper<Key, WindowedSynopsis<S>>,Key, TimeWindow>() {
                    @Override
                    public void apply(Key key, TimeWindow window, Iterable<S> values, Collector<StratifiedSynopsisWrapper<Key, WindowedSynopsis<S>>> out) throws Exception {
                        values.forEach(new Consumer<S>() {
                            @Override
                            public void accept(S synopsis) {
                                WindowedSynopsis windowedSynopsis = new WindowedSynopsis<S>(synopsis, window.getStart(), window.getEnd());
                                out.collect(new StratifiedSynopsisWrapper<Key, WindowedSynopsis<S>>(key, windowedSynopsis));
                            }
                        });
                    }
                }).returns(new TypeHint<StratifiedSynopsisWrapper<Key, WindowedSynopsis<S>>>() {});
    }

    private static <S extends Synopsis, Key extends Serializable, Value> SingleOutputStreamOperator<StratifiedSynopsisWrapper<Key, WindowedSynopsis<S>>> buildScottyStratified
            (BuildConfiguration config) throws Exception {

        if (!StratifiedSynopsis.class.isAssignableFrom(config.synopsisClass)){
            throw new Exception("Synopsis needs to extend from StratifiedSynopsis Abstract Class");
        }

        KeyedStream<Tuple2<Key, Value>, Tuple> keyedStream = config.inputStream.map(config.stratificationKeyExtractor)
                    .keyBy(0);


        // TODO: question: What is SamplerWithTimestamps ?
        // TODO: how to extract Stratification Key form Scotty Processing Functions like in Flink?
        // TODO: do we ever need to use TimeStampedElements?
        KeyedScottyWindowOperator<Tuple, Tuple2<Key, Value>, S> processingFunction;
        if (!MergeableSynopsis.class.isAssignableFrom(config.synopsisClass)){ // Non-Mergeable Synopsis!

            processingFunction = new KeyedScottyWindowOperator<>(new StratifiedNonMergeableSynopsisFunction(config.synopsisClass, config.sliceManagerClass, config.synParams));

        } else if (SamplerWithTimestamps.class.isAssignableFrom(config.synopsisClass)) {

            processingFunction = new KeyedScottyWindowOperator<>(new SynopsisFunction(true, config.synopsisClass, config.synParams));

        } else if (InvertibleSynopsis.class.isAssignableFrom(config.synopsisClass)) {

            processingFunction = new KeyedScottyWindowOperator<>(new InvertibleSynopsisFunction(true, config.synopsisClass, config.synParams));

        } else if (CommutativeSynopsis.class.isAssignableFrom(config.synopsisClass)) {

            processingFunction = new KeyedScottyWindowOperator<>(new CommutativeSynopsisFunction(true, config.synopsisClass, config.synParams));

        } else {

            processingFunction = new KeyedScottyWindowOperator<>(new SynopsisFunction(true, config.synopsisClass, config.synParams));

        }

        for (Window window : config.windows) {
            processingFunction.addWindow(window);
        }

        return keyedStream.process(processingFunction)
                .map(new MapFunction<AggregateWindow<S>, StratifiedSynopsisWrapper<Key, WindowedSynopsis<S>>>() {
                    @Override
                    public StratifiedSynopsisWrapper<Key, WindowedSynopsis<S>> map(AggregateWindow<S> value) throws Exception {
                        StratifiedSynopsis<Key> stratifiedSynopsis = (StratifiedSynopsis) value.getAggValues().get(0);
                        return new StratifiedSynopsisWrapper(stratifiedSynopsis.getPartitionValue(), new WindowedSynopsis(value.getAggValues().get(0), value.getStart(), value.getEnd()));
                    }
                });

    }

    private static <S extends Synopsis> SingleOutputStreamOperator<WindowedSynopsis<S>> buildFlink(BuildConfiguration config){
        // TODO: use this parallelism measure or getParallelismKeys ?!
        int parallelism = config.inputStream.getExecutionEnvironment().getParallelism();

        // TODO: original NonMergeable Synopsis use OrderAndIndex with Parallelism = 1...  check whether this is needed

        DataStream rescaled = config.inputStream.rescale();
        KeyedStream keyBy;
        if(config.miniBatchSize > 1){
            // Non-Mergeable with miniBatchSize (!)
            keyBy = config.inputStream.process(new OrderAndIndex(-1, config.miniBatchSize, getParallelismKeys())).setParallelism(1)
                    .keyBy(0);
        } else if (SamplerWithTimestamps.class.isAssignableFrom(config.synopsisClass)) {
            keyBy = rescaled.process(new ConvertToSample())
                    .assignTimestampsAndWatermarks(new SampleTimeStampExtractor())
                    .map(new AddParallelismIndex())
                    .keyBy(0);
        } else {
            keyBy = rescaled.map(new AddParallelismIndex())
                    .keyBy(0);
        }

        TumblingWindow window = (TumblingWindow) config.windows[0];
        WindowedStream windowedStream = window.getWindowMeasure() == WindowMeasure.Count
                ? keyBy.countWindow(window.getSize() / parallelism)
                : keyBy.timeWindow(Time.milliseconds(window.getSize()));


        boolean mergeable = MergeableSynopsis.class.isAssignableFrom(config.synopsisClass);

        RichAggregateFunction synopsisFunction = mergeable
                ? new SynopsisAggregator(config.synopsisClass, config.synParams)
                : new NonMergeableSynopsisAggregator(config.synopsisClass, config.synParams);

        SingleOutputStreamOperator preAggregated = windowedStream.aggregate(synopsisFunction);


        AllWindowedStream partialAggregate = window.getWindowMeasure() == WindowMeasure.Count
                ? preAggregated.countWindowAll(parallelism)
                : preAggregated.timeWindowAll(Time.milliseconds(window.getSize()));


        if (mergeable) {
            // TODO: check whether the Typing below (MergeableSynopsis vs S) actually works || for Count: use this reduce function whithin another count window or use flatmap as in the original?
            return partialAggregate
                    .reduce(new ReduceFunction<MergeableSynopsis>() {
                        public MergeableSynopsis reduce(MergeableSynopsis value1, MergeableSynopsis value2) throws Exception {
                            return value1.merge(value2);
                        }
                    }, new AddWindowTimes()).returns(new TypeHint<WindowedSynopsis<S>>() {
                    });
        } else {
            // TODO: check whether the NonMergeableSynopsisUnifier also works with the CountWindow!
            return partialAggregate.aggregate(
                    new NonMergeableSynopsisUnifier(config.managerClass),
                    new AddWindowTimes()).returns(new TypeHint<WindowedSynopsis<NonMergeableSynopsisManager>>() {});
        }

    }

    private static<T, S extends Synopsis> SingleOutputStreamOperator<WindowedSynopsis<S>> buildScotty(BuildConfiguration config){

        boolean mergeable = MergeableSynopsis.class.isAssignableFrom(config.synopsisClass);

        DataStream<T> rescaled = config.inputStream.rescale();

        KeyedStream<Tuple2<Integer, Object>, Tuple> keyedStream;
        KeyedScottyWindowOperator<Tuple, Tuple2<Integer, Object>, S> processingFunction;


        if (!mergeable){
            // Non-Mergeable Case

            keyedStream = config.inputStream.process(new OrderAndIndex(-1, config.miniBatchSize, getParallelismKeys())).setMaxParallelism(1)
                    .keyBy(0);

            processingFunction = new KeyedScottyWindowOperator<>(new NonMergeableSynopsisFunction(config.synopsisClass, config.sliceManagerClass, config.synParams));

        } else if (SamplerWithTimestamps.class.isAssignableFrom(config.synopsisClass)) {

            keyedStream = rescaled.process(new ConvertToSample<>())
                    .map(new AddParallelismIndex<>())
                    .keyBy(0);

            processingFunction = new KeyedScottyWindowOperator<>(new SynopsisFunction(config.synopsisClass, config.synParams));

        } else {

            keyedStream = rescaled.map(new AddParallelismIndex())
                    .keyBy(0);

            if (InvertibleSynopsis.class.isAssignableFrom(config.synopsisClass)) {

                processingFunction = new KeyedScottyWindowOperator<>(new InvertibleSynopsisFunction(config.synopsisClass, config.synParams));

            } else if (CommutativeSynopsis.class.isAssignableFrom(config.synopsisClass)) {

                processingFunction = new KeyedScottyWindowOperator<>(new CommutativeSynopsisFunction(config.synopsisClass, config.synParams));

            } else {

                processingFunction = new KeyedScottyWindowOperator<>(new SynopsisFunction(config.synopsisClass, config.synParams));
            }
        }

        for (int i = 0; i < config.windows.length; i++) {
            processingFunction.addWindow(config.windows[i]);
        }

        RichFlatMapFunction<AggregateWindow<S>, AggregateWindow<S>> combineSynopsis = mergeable
                ? new MergePreAggregates(getParallelismKeys())
                : new BuildSynopsis.UnifyToManager(config.managerClass);

        return keyedStream.process(processingFunction)
                .flatMap(combineSynopsis)
                .setParallelism(1)
                .map(new AddWindowTimesScotty());

    }

    static class AddWindowTimes<S extends Synopsis> implements AllWindowFunction<S, WindowedSynopsis<S>, TimeWindow> {
        @Override
        public void apply(TimeWindow window, Iterable values, Collector out) throws Exception {
            values.forEach(new Consumer<S>() {
                @Override
                public void accept(S synopsis) {
                    out.collect(new WindowedSynopsis<S>(synopsis, window.getStart(), window.getEnd()));
                }
            });
        }
    }

    static class AddWindowTimesScotty<S extends Synopsis> implements MapFunction<AggregateWindow<S>, WindowedSynopsis<S>> {
        @Override
        public WindowedSynopsis<S> map(AggregateWindow<S> value) throws Exception {
            return new WindowedSynopsis<S>(value.getAggValues().get(0), value.getStart(), value.getEnd());
        }
    }
}

