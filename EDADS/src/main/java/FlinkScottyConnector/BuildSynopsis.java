package FlinkScottyConnector;

import Synopsis.MergeableSynopsis;
import Synopsis.NonMergeableSynopsisManager;
import Synopsis.Sampling.SampleElement;
import Synopsis.Sampling.SamplerWithTimestamps;
import Synopsis.Synopsis;
import Synopsis.CommutativeSynopsis;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.flinkconnector.KeyedScottyWindowOperator;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import Synopsis.InvertibleSynopsis;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Class to organize the static functions to generate window based Synopses.
 *
 * @author Rudi Poepsel Lemaitre
 */
public final class BuildSynopsis {

    static int parallelismKeys = -1;

    public static void setParallelismKeys(int newParallelismKeys) {
        parallelismKeys = newParallelismKeys;
    }

    /**
     * Build an operator pipeline to generate a stream of time window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#timeWindow} will accumulate the a MergeableSynopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single MergeableSynopsis representing the whole window.
     *
     * @param inputStream   the data stream to build the MergeableSynopsis
     * @param windowTime    the size of the time window
     * @param keyField      the field of the tuple to build the MergeableSynopsis. Set to -1 to build the MergeableSynopsis over the whole tuple.
     * @param synopsisClass the type of MergeableSynopsis to be computed
     * @param parameters    the initialization parameters for the MergeableSynopsis
     * @param <T>           the type of the input elements
     * @param <S>           the type of the MergeableSynopsis
     * @return stream of time window based Synopses
     */
    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> timeBased(DataStream<T> inputStream, Time windowTime, int keyField, Class<S> synopsisClass, Object... parameters) {
        if (SamplerWithTimestamps.class.isAssignableFrom(synopsisClass)) {
            return sampleTimeBased(inputStream, windowTime, keyField, synopsisClass, parameters);
        }
        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters);

        SingleOutputStreamOperator reduce = inputStream
                .map(new AddParallelismIndex(keyField))
                .keyBy(0)
                .timeWindow(windowTime)
                .aggregate(agg)
                .timeWindowAll(windowTime)
                .reduce(new ReduceFunction<S>() { // Merge all sketches in the global window
                    @Override
                    public MergeableSynopsis reduce(MergeableSynopsis value1, MergeableSynopsis value2) throws Exception {
                        MergeableSynopsis merged = value1.merge(value2);
                        return merged;
                    }
                }).returns(synopsisClass);
        return reduce;
    }

    /**
     * Build an operator pipeline to generate a stream of time window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#timeWindow} will accumulate the a MergeableSynopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single MergeableSynopsis representing the whole window.
     *
     * @param inputStream   the data stream to build the MergeableSynopsis
     * @param windowTime    the size of the time window
     * @param keyField      the field of the tuple to build the MergeableSynopsis. Set to -1 to build the MergeableSynopsis over the whole tuple.
     * @param synopsisClass the type of MergeableSynopsis to be computed
     * @param parameters    the initialization parameters for the MergeableSynopsis
     * @param <T>           the type of the input elements
     * @param <S>           the type of the MergeableSynopsis
     * @return stream of time window based Synopses
     */
    public static <T, S extends Synopsis, M extends NonMergeableSynopsisManager> SingleOutputStreamOperator<M> timeBased(DataStream<T> inputStream, Time windowTime, int keyField, Class<S> synopsisClass, Class<M> managerClass, Object... parameters) {
        NonMergeableSynopsisAggregator agg = new NonMergeableSynopsisAggregator(synopsisClass, parameters, keyField);
        SingleOutputStreamOperator parallel = inputStream
                .map(new AddParallelismIndex(keyField));

        SingleOutputStreamOperator reduce = parallel
                .keyBy(0)
                .timeWindow(windowTime)
                .aggregate(agg)
                .timeWindowAll(windowTime)
                .aggregate(new NonMergeableSynopsisUnifier(managerClass))
                .returns(managerClass);
        return reduce;
    }

    /**
     * Build an operator pipeline to generate a stream of time window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#timeWindow} will accumulate the a MergeableSynopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single MergeableSynopsis representing the whole window.
     *
     * @param inputStream   the data stream to build the MergeableSynopsis
     * @param windowTime    the size of the time window
     * @param keyField      the field of the tuple to build the MergeableSynopsis. Set to -1 to build the MergeableSynopsis over the whole tuple.
     * @param synopsisClass the type of MergeableSynopsis to be computed
     * @param parameters    the initialization parameters for the MergeableSynopsis
     * @param <T>           the type of the input elements
     * @param <S>           the type of the MergeableSynopsis
     * @return stream of time window based Synopses
     */
    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> slidingTimeBased(DataStream<T> inputStream, Time windowTime, Time slideTime, int keyField, Class<S> synopsisClass, Object... parameters) {
        if (SamplerWithTimestamps.class.isAssignableFrom(synopsisClass)) {
            return slidingSampleTimeBased(inputStream, windowTime, slideTime, keyField, synopsisClass, parameters);
        }
        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters);

        SingleOutputStreamOperator reduce = inputStream
                .map(new AddParallelismIndex(keyField))
                .keyBy(0)
                .timeWindow(windowTime, slideTime)
                .aggregate(agg)
                .timeWindowAll(windowTime, slideTime)
                .reduce(new ReduceFunction<S>() { // Merge all sketches in the global window
                    @Override
                    public MergeableSynopsis reduce(MergeableSynopsis value1, MergeableSynopsis value2) throws Exception {
                        MergeableSynopsis merged = value1.merge(value2);
                        return merged;
                    }
                }).returns(synopsisClass);
        return reduce;
    }

    /**
     * Build an operator pipeline to generate a stream of time window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#timeWindow} will accumulate the a MergeableSynopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single MergeableSynopsis representing the whole window.
     *
     * @param inputStream   the data stream to build the MergeableSynopsis
     * @param windowTime    the size of the time window
     * @param keyField      the field of the tuple to build the MergeableSynopsis. Set to -1 to build the MergeableSynopsis over the whole tuple.
     * @param synopsisClass the type of MergeableSynopsis to be computed
     * @param parameters    the initialization parameters for the MergeableSynopsis
     * @param <T>           the type of the input elements
     * @param <S>           the type of the MergeableSynopsis
     * @return stream of time window based Synopses
     */
    public static <T, S extends Synopsis, M extends NonMergeableSynopsisManager> SingleOutputStreamOperator<M> slidingTimeBased(DataStream<T> inputStream, Time windowTime, Time slideTime, int keyField, Class<S> synopsisClass, Class<M> managerClass, Object... parameters) {
        NonMergeableSynopsisAggregator agg = new NonMergeableSynopsisAggregator(synopsisClass, parameters, keyField);
        SingleOutputStreamOperator reduce = inputStream
                .map(new AddParallelismIndex(keyField))
                .keyBy(0)
                .timeWindow(windowTime, slideTime)
                .aggregate(agg)
                .timeWindowAll(windowTime, slideTime)
                .aggregate(new NonMergeableSynopsisUnifier(managerClass))
                .returns(managerClass);
        return reduce;
    }


    /**
     * Build an operator pipeline to generate a stream of time window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#timeWindow} will accumulate the a MergeableSynopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single MergeableSynopsis representing the whole window.
     *
     * @param inputStream   the data stream to build the MergeableSynopsis
     * @param windowTime    the size of the time window
     * @param synopsisClass the type of MergeableSynopsis to be computed
     * @param parameters    the initialization parameters for the MergeableSynopsis
     * @param <T>           the type of the input elements
     * @param <S>           the type of the MergeableSynopsis
     * @return stream of time window based Synopses
     */
    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> timeBased(DataStream<T> inputStream, Time windowTime, Class<S> synopsisClass, Object... parameters) {
        return timeBased(inputStream, windowTime, -1, synopsisClass, parameters);
    }

    /**
     * Build an operator pipeline to generate a stream of time window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#timeWindow} will accumulate the a MergeableSynopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single MergeableSynopsis representing the whole window.
     *
     * @param inputStream   the data stream to build the MergeableSynopsis
     * @param windowTime    the size of the time window
     * @param synopsisClass the type of MergeableSynopsis to be computed
     * @param parameters    the initialization parameters for the MergeableSynopsis
     * @param <T>           the type of the input elements
     * @param <S>           the type of the MergeableSynopsis
     * @return stream of time window based Synopses
     */
    public static <T, S extends Synopsis, M extends NonMergeableSynopsisManager> SingleOutputStreamOperator<M> timeBased(DataStream<T> inputStream, Time windowTime, Class<S> synopsisClass, Class<M> managerClass, Object... parameters) {
        return timeBased(inputStream, windowTime, -1, synopsisClass, managerClass, parameters);
    }


    /**
     * Build an operator pipeline to generate a stream of count window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#countWindow} will accumulate the a MergeableSynopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single MergeableSynopsis representing the whole window.
     *
     * @param inputStream   the data stream to build the MergeableSynopsis
     * @param windowSize    the size of the count window
     * @param keyField      the field of the tuple to build the MergeableSynopsis. Set to -1 to build the MergeableSynopsis over the whole tuple.
     * @param synopsisClass the type of MergeableSynopsis to be computed
     * @param parameters    the initialization parameters for the MergeableSynopsis
     * @param <T>           the type of the input elements
     * @param <S>           the type of the MergeableSynopsis
     * @return stream of count window based Synopses
     */
    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> countBased(DataStream<T> inputStream, long windowSize, int keyField, Class<S> synopsisClass, Object... parameters) {
        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters);
        int parallelism = inputStream.getExecutionEnvironment().getParallelism();

        SingleOutputStreamOperator reduce = inputStream
                .map(new AddParallelismIndex(keyField))
                .keyBy(0)
                .countWindow(windowSize / parallelism)
                .aggregate(agg)
                .countWindowAll(parallelism)
                .reduce(new ReduceFunction<S>() { // Merge all sketches in the global window
                    @Override
                    public MergeableSynopsis reduce(MergeableSynopsis value1, MergeableSynopsis value2) throws Exception {
                        return value1.merge(value2);
                    }
                }).returns(synopsisClass);
        return reduce;
    }

    /**
     * Build an operator pipeline to generate a stream of count window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#countWindow} will accumulate the a MergeableSynopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single MergeableSynopsis representing the whole window.
     *
     * @param inputStream   the data stream to build the MergeableSynopsis
     * @param windowSize    the size of the count window
     * @param keyField      the field of the tuple to build the MergeableSynopsis. Set to -1 to build the MergeableSynopsis over the whole tuple.
     * @param synopsisClass the type of MergeableSynopsis to be computed
     * @param parameters    the initialization parameters for the MergeableSynopsis
     * @param <T>           the type of the input elements
     * @param <S>           the type of the MergeableSynopsis
     * @return stream of count window based Synopses
     */
    public static <T, S extends Synopsis, M extends NonMergeableSynopsisManager> SingleOutputStreamOperator<M> countBased(DataStream<T> inputStream, long windowSize, int keyField, Class<S> synopsisClass, Class<M> managerClass, Object... parameters) {
        NonMergeableSynopsisAggregator agg = new NonMergeableSynopsisAggregator(synopsisClass, parameters, keyField);
        int parallelism = inputStream.getExecutionEnvironment().getParallelism();

        SingleOutputStreamOperator reduce = inputStream
                .map(new AddParallelismIndex(keyField))
                .keyBy(0)
                .countWindow(windowSize / parallelism)
                .aggregate(agg)
                .countWindowAll(parallelism)
                .aggregate(new NonMergeableSynopsisUnifier(managerClass))
                .returns(managerClass);
        return reduce;
    }

    /**
     * Build an operator pipeline to generate a stream of count window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#countWindow} will accumulate the a MergeableSynopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single MergeableSynopsis representing the whole window.
     *
     * @param inputStream   the data stream to build the MergeableSynopsis
     * @param windowSize    the size of the count window
     * @param synopsisClass the type of MergeableSynopsis to be computed
     * @param parameters    the initialization parameters for the MergeableSynopsis
     * @param <T>           the type of the input elements
     * @param <S>           the type of the MergeableSynopsis
     * @return stream of count window based Synopses
     */
    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> countBased(DataStream<T> inputStream, long windowSize, Class<S> synopsisClass, Object... parameters) {
        return countBased(inputStream, windowSize, -1, synopsisClass, parameters);
    }

    /**
     * Build an operator pipeline to generate a stream of count window based Synopses. Firstly each element will be
     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
     * {@link KeyedStream#countWindow} will accumulate the a MergeableSynopsis via the {@link SynopsisAggregator}. Afterwards
     * the partial results of the partitions will be reduced (merged) to a single MergeableSynopsis representing the whole window.
     *
     * @param inputStream   the data stream to build the MergeableSynopsis
     * @param windowSize    the size of the count window
     * @param synopsisClass the type of MergeableSynopsis to be computed
     * @param parameters    the initialization parameters for the MergeableSynopsis
     * @param <T>           the type of the input elements
     * @param <S>           the type of the MergeableSynopsis
     * @return stream of count window based Synopses
     */
    public static <T, S extends MergeableSynopsis, M extends NonMergeableSynopsisManager> SingleOutputStreamOperator<M> countBased(DataStream<T> inputStream, long windowSize, Class<S> synopsisClass, Class<M> managerClass, Object... parameters) {
        return countBased(inputStream, windowSize, -1, synopsisClass, managerClass, parameters);
    }


    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> sampleTimeBased(DataStream<T> inputStream, Time windowTime, int keyField, Class<S> synopsisClass, Object... parameters) {
        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters);
        SingleOutputStreamOperator reduce1 = inputStream
                .process(new ConvertToSample(keyField))
                .assignTimestampsAndWatermarks(new SampleTimeStampExtractor())
                .map(new AddParallelismIndex(keyField))
                .keyBy(0)
                .timeWindow(windowTime)
                .aggregate(agg);
        SingleOutputStreamOperator reduce = reduce1.timeWindowAll(windowTime)
                .reduce(new ReduceFunction<S>() { // Merge all sketches in the global window
                    @Override
                    public MergeableSynopsis reduce(MergeableSynopsis value1, MergeableSynopsis value2) throws Exception {
                        return value1.merge(value2);
                    }
                }).returns(synopsisClass);
        return reduce;
    }

    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> slidingSampleTimeBased(DataStream<T> inputStream, Time windowTime, Time slideTime, int keyField, Class<S> synopsisClass, Object... parameters) {
        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters);
        SingleOutputStreamOperator reduce1 = inputStream
                .process(new ConvertToSample(keyField))
                .assignTimestampsAndWatermarks(new SampleTimeStampExtractor())
                .map(new AddParallelismIndex(keyField))
                .keyBy(0)
                .timeWindow(windowTime, slideTime)
                .aggregate(agg);
        SingleOutputStreamOperator reduce = reduce1.timeWindowAll(windowTime, slideTime)
                .reduce(new ReduceFunction<S>() { // Merge all sketches in the global window
                    @Override
                    public MergeableSynopsis reduce(MergeableSynopsis value1, MergeableSynopsis value2) throws Exception {
                        return value1.merge(value2);
                    }
                }).returns(synopsisClass);
        return reduce;
    }

    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<AggregateWindow<S>> scottyWindows(DataStream<T> inputStream, Window[] windows, int keyField, Class<S> synopsisClass, Object... parameters) {
        if (SamplerWithTimestamps.class.isAssignableFrom(synopsisClass)) {
            KeyedStream<Tuple2<Integer, Object>, Tuple> keyedStream = inputStream.process(new ConvertToSample<>(keyField)).map(new AddParallelismIndex<>(-1)).keyBy(0);
            KeyedScottyWindowOperator<Tuple, Tuple2<Integer, Object>, S> processingFunction =
                    new KeyedScottyWindowOperator<>(new SynopsisFunction(synopsisClass, parameters));
            for (int i = 0; i < windows.length; i++) {
                processingFunction.addWindow(windows[i]);
            }
            return keyedStream.process(processingFunction)
                    .flatMap(new MergePreAggregates())
                    .setParallelism(1);
        } else {
            KeyedStream<Tuple2<Integer, Object>, Tuple> keyedStream = inputStream.map(new AddParallelismIndex<>(keyField)).keyBy(0);
            KeyedScottyWindowOperator<Tuple, Tuple2<Integer, Object>, S> processingFunction;
            if (InvertibleSynopsis.class.isAssignableFrom(synopsisClass)) {
                processingFunction =
                        new KeyedScottyWindowOperator<>(new InvertibleSynopsisFunction(synopsisClass, parameters));
            } else if (CommutativeSynopsis.class.isAssignableFrom(synopsisClass)) {
                processingFunction =
                        new KeyedScottyWindowOperator<>(new CommutativeSynopsisFunction(synopsisClass, parameters));
            } else {
                processingFunction =
                        new KeyedScottyWindowOperator<>(new SynopsisFunction(synopsisClass, parameters));
            }
            for (int i = 0; i < windows.length; i++) {
                processingFunction.addWindow(windows[i]);
            }
            return keyedStream.process(processingFunction)
                    .flatMap(new MergePreAggregates())
                    .setParallelism(1);
        }
    }

    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<AggregateWindow<S>> scottyWindows(DataStream<T> inputStream, Window[] windows, Class<S> synopsisClass, Object... parameters) {
        return scottyWindows(inputStream, windows, -1, synopsisClass, parameters);
    }

    public static <T, S extends Synopsis, SM extends NonMergeableSynopsisManager, M extends NonMergeableSynopsisManager> SingleOutputStreamOperator<AggregateWindow<M>> scottyWindows(DataStream<T> inputStream, Window[] windows, int keyField, Class<S> synopsisClass, Class<SM> sliceManagerClass, Class<M> managerClass, Object... parameters) {

        KeyedStream<Tuple2<Integer, Object>, Tuple> keyedStream = inputStream.map(new AddParallelismIndex<>(keyField)).keyBy(0);
        KeyedScottyWindowOperator<Tuple, Tuple2<Integer, Object>, NonMergeableSynopsisManager> processingFunction =
                new KeyedScottyWindowOperator<>(new NonMergeableSynopsisFunction(keyField, -1, synopsisClass, sliceManagerClass, parameters));

        for (int i = 0; i < windows.length; i++) {
            processingFunction.addWindow(windows[i]);
        }
        return keyedStream.process(processingFunction)
                .flatMap(new UnifyToManager<M>(managerClass))
                .setParallelism(1);
    }


    /**
     * Integer state for Stateful Functions
     */
    public static class IntegerState implements ValueState<Integer>, Serializable {
        int value;

        @Override
        public Integer value() throws IOException {
            return value;
        }

        @Override
        public void update(Integer value) throws IOException {
            this.value = value;
        }

        @Override
        public void clear() {
            value = 0;
        }

        private void writeObject(java.io.ObjectOutputStream out) throws IOException {
            out.writeInt(value);
        }


        private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
            this.value = in.readInt();
        }

        private void readObjectNoData() throws ObjectStreamException {
            throw new NotSerializableException("Serialization error in class " + this.getClass().getName());
        }
    }

    /**
     * Integer state for Stateful Functions
     */
    public static class WindowState implements ValueState<HashMap<WindowID, Tuple2<Integer, AggregateWindow<MergeableSynopsis>>>> {
        HashMap<WindowID, Tuple2<Integer, AggregateWindow<MergeableSynopsis>>> openWindows;
        int numberKeys;

        public WindowState(int numberKeys) {
            this.numberKeys = numberKeys;
            this.openWindows = new HashMap<>();
        }

        @Override
        public HashMap value() throws IOException {
            return openWindows;
        }

        @Override
        public void update(HashMap value) throws IOException {
            this.openWindows = value;
        }

        @Override
        public void clear() {
            openWindows.clear();
        }
    }

    public static class WindowID implements Comparable {
        private long start;
        private long end;

        public WindowID(long start, long end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public int compareTo(Object o) {
            if (o instanceof WindowID) {
                if (((WindowID) o).start > this.start) {
                    return -1;
                } else if (((WindowID) o).start < this.start) {
                    return 1;
                } else if (((WindowID) o).start == this.start && ((WindowID) o).end > this.end) {
                    return -1;
                } else if (((WindowID) o).start == this.start && ((WindowID) o).end < this.end) {
                    return 1;
                } else /*if(((WindowID) o).start == this.start && ((WindowID) o).end == this.end)*/ {
                    return 0;
                }
            }
            throw new IllegalArgumentException("Must be a WindowID to be compared");
        }


        @Override
        public boolean equals(Object o) {
            if (o instanceof WindowID) {
                if (((WindowID) o).start == this.start && ((WindowID) o).end == this.end) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public int hashCode() {
            int result = (int) (start ^ (start >>> 32));
            result = 31 * result + (int) (end ^ (end >>> 32));
            return result;
        }
    }


    public static class MergePreAggregates<S extends MergeableSynopsis> extends RichFlatMapFunction<AggregateWindow<S>, AggregateWindow<S>> {

        WindowState state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = new WindowState(parallelismKeys);
        }

        @Override
        public void flatMap(AggregateWindow<S> value, Collector<AggregateWindow<S>> out) throws Exception {
            HashMap<WindowID, Tuple2<Integer, AggregateWindow<S>>> openWindows = state.value();
            WindowID windowID = new WindowID(value.getStart(), value.getEnd());
            Tuple2<Integer, AggregateWindow<S>> synopsisAggregateWindow = openWindows.get(windowID);
            if (synopsisAggregateWindow == null) {
                openWindows.put(windowID, new Tuple2<>(1, value));
            } else if (synopsisAggregateWindow.f0 == parallelismKeys - 1) {
                synopsisAggregateWindow.f1.getAggValues().get(0).merge(value.getAggValues().get(0));
                out.collect(synopsisAggregateWindow.f1);
                openWindows.remove(windowID);
            } else {
                synopsisAggregateWindow.f1.getAggValues().get(0).merge(value.getAggValues().get(0));
                synopsisAggregateWindow.f0 += 1;
            }
            state.update(openWindows);
        }

    }


    public static class UnifyToManager<M extends NonMergeableSynopsisManager> extends RichFlatMapFunction<AggregateWindow<NonMergeableSynopsisManager>, AggregateWindow<M>> {

        WindowState state;
        Class<M> managerClass;

        public UnifyToManager(Class<M> managerClass){
            this.managerClass = managerClass;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            state = new WindowState(parallelismKeys);
        }

        @Override
        public void flatMap(AggregateWindow<NonMergeableSynopsisManager> value, Collector<AggregateWindow<M>> out) throws Exception {
            HashMap<WindowID, Tuple2<Integer, AggregateWindow<M>>> openWindows = state.value();
            WindowID windowID = new WindowID(value.getStart(), value.getEnd());
            Tuple2<Integer, AggregateWindow<M>> synopsisAggregateWindow = openWindows.get(windowID);
            if (synopsisAggregateWindow == null) {
                NonMergeableSynopsisWindowState<M> aggWindow = new NonMergeableSynopsisWindowState(value, managerClass);
                openWindows.put(windowID, new Tuple2<>(1, aggWindow));
            } else if (synopsisAggregateWindow.f0 == parallelismKeys - 1) {
                synopsisAggregateWindow.f1.getAggValues().get(0).addSynopsis(value.getAggValues().get(0));
                out.collect(synopsisAggregateWindow.f1);
                openWindows.remove(windowID);
            } else {
                synopsisAggregateWindow.f1.getAggValues().get(0).addSynopsis(value.getAggValues().get(0));
                synopsisAggregateWindow.f0 += 1;
            }
            state.update(openWindows);
        }

    }

    public static class NonMergeableSynopsisWindowState<M extends NonMergeableSynopsisManager> implements AggregateWindow<M>{
        private final long start;
        private final long endTs;
        private final WindowMeasure measure;
        private List<M> aggValues;

        public NonMergeableSynopsisWindowState(AggregateWindow<Synopsis> aggWindow, Class<M> managerClass){
            this(aggWindow.getStart(), aggWindow.getEnd(), aggWindow.getMeasure());
            Constructor<M> constructor;
            try {
                constructor = managerClass.getConstructor();
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
            }
            for (int i = 0; i < aggWindow.getAggValues().size(); i++) {
                M manager;
                try {
                    manager = constructor.newInstance();
                } catch (InstantiationException e) {
                    throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
                } catch (InvocationTargetException e) {
                    throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
                }
                manager.addSynopsis(aggWindow.getAggValues().get(i));
                aggValues.add(manager);
            }
        }

        public NonMergeableSynopsisWindowState(long start, long endTs, WindowMeasure measure) {
            this.start = start;
            this.endTs = endTs;
            this.measure = measure;
            this.aggValues = new ArrayList<>();
        }

        @Override
        public WindowMeasure getMeasure() {
            return measure;
        }

        @Override
        public long getStart() {
            return start;
        }

        @Override
        public long getEnd() {
            return endTs;
        }

        @Override
        public List<M> getAggValues() {
            return aggValues;
        }

        @Override
        public boolean hasValue() {
            return !aggValues.isEmpty();
        }
    }

    /**
     * Stateful map functions to add the parallelism variable
     *
     * @param <T0> type of input elements
     */
    public static class AddParallelismIndex<T0> extends RichMapFunction<T0, Tuple2<Integer, Object>> {

        private transient ValueState<Integer> state;
        public int keyField;
        private Tuple2<Integer, Object> newTuple;

        public AddParallelismIndex(int keyField) {
            this.keyField = keyField;
            if (keyField == -1){
                newTuple = new Tuple2<Integer, Object>();
            } else {
                newTuple = new Tuple2<Integer, Object>();
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            if (parallelismKeys < 1) {
                setParallelismKeys(this.getRuntimeContext().getNumberOfParallelSubtasks());
            }
            state = new IntegerState();
        }

        @Override
        public Tuple2<Integer, Object> map(T0 value) throws Exception {
            int currentNode = state.value();
            int next = currentNode + 1;
            next = next % parallelismKeys;
            state.update(next);

            if (value instanceof Tuple && keyField != -1){
                newTuple.setField(((Tuple) value).getField(keyField), 1);
            } else {
                newTuple.setField(value, 1);
            }
            newTuple.setField(currentNode, 0);
            return newTuple;
        }
    }


    public static class ConvertToSample<T>
            extends ProcessFunction<T, SampleElement> {
        private int keyField = -1;

        public ConvertToSample(int keyField) {
            this.keyField = keyField;
        }

        public ConvertToSample() {
        }

        @Override
        public void processElement(T value, Context ctx, Collector<SampleElement> out) throws Exception {
            if (keyField >= 0 && value instanceof Tuple) {
                SampleElement sample = new SampleElement<>(((Tuple) value).getField(keyField), ctx.timestamp() != null ? ctx.timestamp() : ctx.timerService().currentProcessingTime());
                out.collect(sample);
            } else {
                SampleElement<T> sample = new SampleElement<>(value, ctx.timestamp() != null ? ctx.timestamp() : ctx.timerService().currentProcessingTime());
                out.collect(sample);
            }
        }
    }

    /**
     * The Custom TimeStampExtractor which is used to assign Timestamps and Watermarks for our data
     */
    public static class SampleTimeStampExtractor implements AssignerWithPunctuatedWatermarks<SampleElement> {
        /**
         * Asks this implementation if it wants to emit a watermark. This method is called right after
         * the {@link #extractTimestamp(SampleElement, long)}   method.
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
        public Watermark checkAndGetNextWatermark(SampleElement lastElement, long extractedTimestamp) {
            return new Watermark(extractedTimestamp);
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
        public long extractTimestamp(SampleElement element, long previousElementTimestamp) {
            return element.getTimeStamp();
        }
    }

    public static class NonMergeableSynopsisUnifier<S extends Synopsis> implements AggregateFunction<S, NonMergeableSynopsisManager, NonMergeableSynopsisManager> {
        private Class<? extends NonMergeableSynopsisManager> managerClass;

        public NonMergeableSynopsisUnifier(Class<? extends NonMergeableSynopsisManager> managerClass) {
            this.managerClass = managerClass;
        }

        @Override
        public NonMergeableSynopsisManager createAccumulator() {
            Constructor<? extends NonMergeableSynopsisManager> constructor = null;
            try {
                constructor = managerClass.getConstructor();
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
            }
            NonMergeableSynopsisManager manager = null;
            try {
                manager = constructor.newInstance();
            } catch (InstantiationException e) {
                throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
            } catch (IllegalAccessException e) {
                throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
            } catch (InvocationTargetException e) {
                throw new RuntimeException("An unexpected error happen, while unifying the partial results.");
            }
            return manager;
        }

        @Override
        public NonMergeableSynopsisManager add(S value, NonMergeableSynopsisManager accumulator) {
            accumulator.addSynopsis(value);
            return accumulator;
        }

        @Override
        public NonMergeableSynopsisManager getResult(NonMergeableSynopsisManager accumulator) {
            return accumulator;
        }

        @Override
        public NonMergeableSynopsisManager merge(NonMergeableSynopsisManager a, NonMergeableSynopsisManager b) {
            for (int i = 0; i < b.getUnifiedSynopses().size(); i++) {
                a.addSynopsis((S) b.getUnifiedSynopses().get(i));
            }
            return a;
        }
    }

//    /**
//     * Debug function to print the output of the aggregators.
//     * Build an operator pipeline to generate a stream of time window based Synopses. Firstly each element will be
//     * assigned to a random partition. Then based on the partition a {@link KeyedStream} will be generated and an
//     * {@link KeyedStream#timeWindow} will accumulate the a MergeableSynopsis via the {@link SynopsisAggregator}. Afterwards
//     * the partial results of the partitions will be reduced (merged) to a single MergeableSynopsis representing the whole window.
//     *
//     * @param inputStream   the data stream to build the MergeableSynopsis
//     * @param windowTime    the size of the time window
//     * @param keyField      the field of the tuple to build the MergeableSynopsis. Set to -1 to build the MergeableSynopsis over the whole tuple.
//     * @param synopsisClass the type of MergeableSynopsis to be computed
//     * @param parameters    the initialization parameters for the MergeableSynopsis
//     * @param <T>           the type of the input elements
//     * @param <S>           the type of the MergeableSynopsis
//     * @return stream of time window based Synopses
//     */
//    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> debugAggimeBased(DataStream<T> inputStream, Time windowTime, int keyField, Class<S> synopsisClass, Object... parameters) {
//        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters, keyField);
//
//        SingleOutputStreamOperator reduce1 = inputStream
//                .map(new AddParallelismIndex())
//                .keyBy(0)
//                .timeWindow(windowTime)
//                .aggregate(agg);
//        reduce1.writeAsText("output/aggregators", FileSystem.WriteMode.OVERWRITE);
//        SingleOutputStreamOperator reduce = reduce1
//                .timeWindowAll(windowTime)
//                .reduce(new ReduceFunction<S>() { // Merge all sketches in the global window
//                    @Override
//                    public MergeableSynopsis reduce(MergeableSynopsis value1, MergeableSynopsis value2) throws Exception {
//                        MergeableSynopsis merged = value1.merge(value2);
//                        return merged;
//                    }
//                }).returns(synopsisClass);
//        return reduce;
//    }
}
