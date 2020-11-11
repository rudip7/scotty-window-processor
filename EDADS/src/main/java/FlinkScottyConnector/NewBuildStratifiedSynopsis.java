package FlinkScottyConnector;

import FlinkScottyConnector.Configs.BuildScottyConfig;
import FlinkScottyConnector.Configs.CountBasedConfig;
import FlinkScottyConnector.Configs.TimeBasedConfig;
import FlinkScottyConnector.FunctionClasses.*;
import Synopsis.*;
import Synopsis.Sampling.SamplerWithTimestamps;
import Synopsis.Sampling.TimestampedElement;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.flinkconnector.KeyedScottyWindowOperator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.function.Consumer;

public class NewBuildStratifiedSynopsis {
    private static int parallelismKeys = -1;

    public static void setParallelismKeys(int newParallelismKeys) {
        parallelismKeys = newParallelismKeys;
        System.out.println("BuildStratifiedSynopsis Parallelism Keys changed to: " + parallelismKeys);
    }

    public static int getParallelismKeys() {
        return parallelismKeys;
    }


    /**
     * @param stratificationKeyExtractor    the Map Function which extracts the stratification key from the inputStream
     * @param <Key>     The stratification key type
     * @return
     */
    public static <T, S extends Synopsis, Key extends Serializable, Value> SingleOutputStreamOperator<StratifiedSynopsisWrapper<Key, WindowedSynopsis<S>>> timeBased
            (DataStream<T> inputStream, RichMapFunction<T, Tuple2<Key, Value>> stratificationKeyExtractor, TimeBasedConfig config) {

        if (MergeableSynopsis.class.isAssignableFrom(config.synopsisClass)) {
            SynopsisAggregator agg = new SynopsisAggregator(true, config.synopsisClass, config.synParams);

            KeyedStream keyBy = inputStream
                    .map(stratificationKeyExtractor)
                    .keyBy(0);

            WindowedStream windowedStream;
            if (config.slideTime == null) {
                windowedStream = keyBy.timeWindow(config.windowTime);
            } else {
                windowedStream = keyBy.timeWindow(config.windowTime, config.slideTime);
            }

            return windowedStream
                    .aggregate(agg, new WindowFunction<S, StratifiedSynopsisWrapper<Key, WindowedSynopsis<S>>,Key, TimeWindow>() {
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
                    }).returns(StratifiedSynopsisWrapper.class);
        } else {
            NonMergeableSynopsisAggregator agg = new NonMergeableSynopsisAggregator(true, config.synopsisClass, config.synParams);
            KeyedStream keyBy = inputStream
                    .map(stratificationKeyExtractor)
                    .keyBy(0);

            WindowedStream windowedStream;
            if (config.slideTime == null) {
                windowedStream = keyBy.timeWindow(config.windowTime);
            } else {
                windowedStream = keyBy.timeWindow(config.windowTime, config.slideTime);
            }

            return windowedStream
                    .aggregate(agg, new WindowFunction<S, StratifiedSynopsisWrapper<Key, WindowedSynopsis<S>>,Key, TimeWindow>() {
                        @Override
                        public void apply(Key key, TimeWindow window, Iterable<S> values, Collector<StratifiedSynopsisWrapper<Key, WindowedSynopsis<S>>> out) throws Exception {
                            // System.out.println("HOLAAAAA");
                            values.forEach(new Consumer<S>() {
                                @Override
                                public void accept(S synopsis) {
                                    WindowedSynopsis<S> windowedSynopsis = new WindowedSynopsis<S>(synopsis, window.getStart(), window.getEnd());
                                    out.collect(new StratifiedSynopsisWrapper<Key, WindowedSynopsis<S>>(key, windowedSynopsis));
                                }
                            });
                        }
                    }).returns(StratifiedSynopsisWrapper.class);
        }
    }


    public static <T, S extends Synopsis, Key, Value> SingleOutputStreamOperator<S> timeBasedWithoutWindowTimes
            (DataStream<T> inputStream, RichMapFunction<T, Tuple2<Key, Value>> stratificationKeyExtractor, TimeBasedConfig config){
        if (MergeableSynopsis.class.isAssignableFrom(config.synopsisClass)) {
            SynopsisAggregator agg = new SynopsisAggregator(true, config.synopsisClass, config.synParams);

            KeyedStream keyBy = inputStream
                    .map(stratificationKeyExtractor)
                    .keyBy(0);

            WindowedStream windowedStream;
            if (config.slideTime == null) {
                windowedStream = keyBy.timeWindow(config.windowTime);
            } else {
                windowedStream = keyBy.timeWindow(config.windowTime, config.slideTime);
            }

            return windowedStream
                    .aggregate(agg)
                    .returns(config.synopsisClass);
        } else {
            NonMergeableSynopsisAggregator agg = new NonMergeableSynopsisAggregator(true, config.synopsisClass, config.synParams);
            KeyedStream keyBy = inputStream
                    .map(stratificationKeyExtractor)
                    .keyBy(0);

            WindowedStream windowedStream;
            if (config.slideTime == null) {
                windowedStream = keyBy.timeWindow(config.windowTime);
            } else {
                windowedStream = keyBy.timeWindow(config.windowTime, config.slideTime);
            }

            return windowedStream
                    .aggregate(agg)
                    .returns(config.synopsisClass);
        }
    }


    public static <T, S extends MergeableSynopsis, Key, Value> SingleOutputStreamOperator<S> countBased
            (DataStream<T> inputStream, RichMapFunction<T, TimestampedElement<Tuple2<Key, Value>>> stratificationKeyExtractor, CountBasedConfig config){
        if (!StratifiedSynopsis.class.isAssignableFrom(config.synopsisClass)) {
            throw new IllegalArgumentException("Synopsis class needs to extend the StratifiedSynopsis abstract class to build a stratified synopsis.");
        }
        SynopsisAggregator agg = new SynopsisAggregator(true, config.synopsisClass, config.synParams);

        KeyedStream keyBy = inputStream
                .map(stratificationKeyExtractor)
                .keyBy(0);

        WindowedStream windowedStream;
        if (config.slideSize == -1) {
            windowedStream = keyBy.countWindow(config.windowSize);
        } else {
            windowedStream = keyBy.countWindow(config.windowSize, config.slideSize);
        }

        return windowedStream
                .aggregate(agg)
                .returns(config.synopsisClass);
    }

    public static <T, S extends MergeableSynopsis, Key, Value> SingleOutputStreamOperator<AggregateWindow<S>> scottyWindows
            (DataStream<T> inputStream, RichMapFunction<T, Tuple2<Key, Value>> stratificationKeyExtractor, BuildScottyConfig config) {
        if (!StratifiedSynopsis.class.isAssignableFrom(config.synopsisClass)) {
            throw new IllegalArgumentException("Synopsis class needs to extend the StratifiedSynopsis abstract class to build a stratified synopsis.");
        }
        if (SamplerWithTimestamps.class.isAssignableFrom(config.synopsisClass)) {
            KeyedStream<Tuple2<Key, Value>, Tuple> keyedStream = inputStream.map(stratificationKeyExtractor)
                    .keyBy(0);

            KeyedScottyWindowOperator<Tuple, Tuple2<Key, Value>, S> processingFunction =
                    new KeyedScottyWindowOperator<>(new SynopsisFunction(true, config.synopsisClass, config.synParams));

            for (Window window : config.windows) {
                processingFunction.addWindow(window);
            }
            return keyedStream.process(processingFunction);
        } else {
            KeyedStream<Tuple2<Key, Value>, Tuple> keyedStream = inputStream.map(stratificationKeyExtractor).keyBy(0);
            KeyedScottyWindowOperator<Tuple, Tuple2<Key, Value>, S> processingFunction;
            if (InvertibleSynopsis.class.isAssignableFrom(config.synopsisClass)) {
                processingFunction =
                        new KeyedScottyWindowOperator<>(new InvertibleSynopsisFunction(true, config.synopsisClass, config.synParams));
            } else if (CommutativeSynopsis.class.isAssignableFrom(config.synopsisClass)) {
                processingFunction =
                        new KeyedScottyWindowOperator<>(new CommutativeSynopsisFunction(config.synopsisClass, config.synParams));
            } else {
                processingFunction =
                        new KeyedScottyWindowOperator<>(new SynopsisFunction(true, config.synopsisClass, config.synParams));
            }
            for (Window window : config.windows) {
                processingFunction.addWindow(window);
            }
            return keyedStream.process(processingFunction);
        }

    }

    public static <T, S extends Synopsis, SM extends NonMergeableSynopsisManager, Key, Value> SingleOutputStreamOperator<AggregateWindow<SM>> nonMergeableScottyWindows
            (DataStream<T> inputStream, RichMapFunction<T, TimestampedElement<Tuple2<Key, Value>>> stratificationKeyExtractor, BuildScottyConfig config) {

        KeyedStream<TimestampedElement<Tuple2<Key, Value>>, String> keyedStream = inputStream
                .map(stratificationKeyExtractor)
                .keyBy(new KeySelector<TimestampedElement<Tuple2<Key, Value>>, String>() {
                    @Override
                    public String getKey(TimestampedElement<Tuple2<Key, Value>> value) throws Exception {
                        return String.valueOf(value.getValue().f0);
                    }
                });

        KeyedScottyWindowOperator<String, TimestampedElement<Tuple2<Key, Value>>, SM> processingFunction =
                new KeyedScottyWindowOperator<>(new StratifiedNonMergeableSynopsisFunction(config.synopsisClass, config.sliceManagerClass, config.synParams));

        for (Window window : config.windows) {
            processingFunction.addWindow(window);
        }
        return keyedStream.process(processingFunction);
    }

    //TODO: all methods which have a partitionField argument (instead of stratificationKeyExtractor) and use the 'ConvertToSample' Function to

    @Deprecated
    public static <T extends Tuple, S extends MergeableSynopsis> SingleOutputStreamOperator<S> timeBased
            (DataStream<T> inputStream, Time windowTime, Time slideTime, int partitionField, int keyField, Class<S> synopsisClass, Object... parameters) {
        if (!StratifiedSynopsis.class.isAssignableFrom(synopsisClass)) {
            throw new IllegalArgumentException("Synopsis class needs to extend the StratifiedSynopsis abstract class to build a stratified synopsis.");
        }
        SynopsisAggregator agg = new SynopsisAggregator(true, synopsisClass, parameters);

        KeyedStream keyBy;

        if (SamplerWithTimestamps.class.isAssignableFrom(synopsisClass)) {
            keyBy = inputStream
                    .process(new ConvertToStratifiedSample<>(partitionField, keyField))
                    .assignTimestampsAndWatermarks(new StratifiedSampleTimeStampExtractor())
                    .keyBy(0);
        } else {
            keyBy = inputStream
                    .map(new TransformStratified<>(partitionField, keyField))
                    .keyBy(0);
        }

        WindowedStream windowedStream;
        if (slideTime == null) {
            windowedStream = keyBy.timeWindow(windowTime);
        } else {
            windowedStream = keyBy.timeWindow(windowTime, slideTime);
        }

        return windowedStream
                .aggregate(agg)
                .returns(synopsisClass);
    }}
    }

}
