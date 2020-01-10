package FlinkScottyConnector;

import Synopsis.MergeableSynopsis;
import Synopsis.NonMergeableSynopsis;
import Synopsis.Sampling.SampleElement;
import Synopsis.Sampling.SamplerWithTimestamps;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.flinkconnector.KeyedScottyWindowOperator;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import Synopsis.InvertibleSynopsis;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;

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
        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters, keyField);

        SingleOutputStreamOperator reduce = inputStream
                .map(new AddParallelismIndex())
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
    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> slidingTimeBased(DataStream<T> inputStream, Time windowTime, Time slideTime, int keyField, Class<S> synopsisClass, Object... parameters) {
        if (SamplerWithTimestamps.class.isAssignableFrom(synopsisClass)) {
            return slidingSampleTimeBased(inputStream, windowTime, slideTime, keyField, synopsisClass, parameters);
        }
        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters, keyField);

        SingleOutputStreamOperator reduce = inputStream
                .map(new AddParallelismIndex())
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
        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters, keyField);
        int parallelism = inputStream.getExecutionEnvironment().getParallelism();

        SingleOutputStreamOperator reduce = inputStream
                .map(new AddParallelismIndex())
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
     * @param synopsisClass the type of MergeableSynopsis to be computed
     * @param parameters    the initialization parameters for the MergeableSynopsis
     * @param <T>           the type of the input elements
     * @param <S>           the type of the MergeableSynopsis
     * @return stream of count window based Synopses
     */
    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> countBased(DataStream<T> inputStream, long windowSize, Class<S> synopsisClass, Object... parameters) {
        return countBased(inputStream, windowSize, -1, synopsisClass, parameters);
    }


    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> sampleTimeBased(DataStream<T> inputStream, Time windowTime, int keyField, Class<S> synopsisClass, Object... parameters) {
        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters, keyField);
        SingleOutputStreamOperator reduce1 = inputStream
                .process(new ConvertToSample(keyField))
                .assignTimestampsAndWatermarks(new SampleTimeStampExtractor())
                .map(new AddParallelismIndex())
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
        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters, keyField);
        SingleOutputStreamOperator reduce1 = inputStream
                .process(new ConvertToSample(keyField))
                .assignTimestampsAndWatermarks(new SampleTimeStampExtractor())
                .map(new AddParallelismIndex())
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


    public static <T extends Tuple, S extends MergeableSynopsis> SingleOutputStreamOperator<AggregateWindow<S>> scottyWindows(DataStream<T> inputStream, Window[] windows, int keyField, Class<S> synopsisClass, Object... parameters) {
        if (SamplerWithTimestamps.class.isAssignableFrom(synopsisClass)) {
            KeyedStream<Tuple2<Integer, SampleElement>, Tuple> keyedStream = inputStream.process(new ConvertToSample<>(keyField)).map(new AddParallelismIndex<>()).keyBy(0);
            KeyedScottyWindowOperator<Tuple, Tuple2<Integer, SampleElement>, S> processingFunction =
                    new KeyedScottyWindowOperator<>(new SynopsisFunction(keyField, -1, synopsisClass, parameters));
            for (int i = 0; i < windows.length; i++) {
                processingFunction.addWindow(windows[i]);
            }
            return keyedStream.process(processingFunction)
                    .flatMap(new MergePreAggregates())
                    .setParallelism(1);
        } else {
            KeyedStream<Tuple2<Integer, T>, Tuple> keyedStream = inputStream.map(new AddParallelismIndex<>()).keyBy(0);
            KeyedScottyWindowOperator<Tuple, Tuple2<Integer, T>, S> processingFunction;
            if (InvertibleSynopsis.class.isAssignableFrom(synopsisClass)) {
                processingFunction =
                        new KeyedScottyWindowOperator<>(new InvertibleSynopsisFunction(keyField,-1, synopsisClass, parameters));
            } else {
                processingFunction =
                        new KeyedScottyWindowOperator<>(new SynopsisFunction(keyField, -1, synopsisClass, parameters));
            }
            for (int i = 0; i < windows.length; i++) {
                processingFunction.addWindow(windows[i]);
            }
            return keyedStream.process(processingFunction)
                    .flatMap(new MergePreAggregates())
                    .setParallelism(1);
        }
    }

    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<AggregateWindow<S>> scottyStratifiedSynopsis(DataStream<T> inputStream, Window[] windows, int partitionField, int keyField, Class<S> synopsisClass, Object... parameters) {
        if (!inputStream.getType().isTupleType()) {
            throw new IllegalArgumentException("Input stream must be of type Tuple.");
        }
        if (partitionField > inputStream.getType().getArity() || partitionField < 0) {
            throw new IllegalArgumentException("Partition field to execute the stratified sampling is not valid.");
        }
        if (SamplerWithTimestamps.class.isAssignableFrom(synopsisClass)) {
            KeyedStream<SampleElement, Tuple> keyedStream = inputStream.process(new ConvertToSample<>(keyField)).keyBy(partitionField);
            KeyedScottyWindowOperator<Tuple, SampleElement, S> processingFunction =
                    new KeyedScottyWindowOperator<>(new SynopsisFunction(keyField, partitionField, synopsisClass, parameters));
            for (int i = 0; i < windows.length; i++) {
                processingFunction.addWindow(windows[i]);
            }
            return keyedStream.process(processingFunction);
        } else {
            KeyedStream<T, Tuple> keyedStream = inputStream.keyBy(partitionField);
            KeyedScottyWindowOperator<Tuple, T, S> processingFunction;
            if (InvertibleSynopsis.class.isAssignableFrom(synopsisClass)) {
                processingFunction =
                        new KeyedScottyWindowOperator<>(new InvertibleSynopsisFunction(keyField, partitionField, synopsisClass, parameters));
            } else {
                processingFunction =
                        new KeyedScottyWindowOperator<>(new SynopsisFunction(keyField, partitionField, synopsisClass, parameters));
            }
            for (int i = 0; i < windows.length; i++) {
                processingFunction.addWindow(windows[i]);
            }
            return keyedStream.process(processingFunction);
        }
    }

    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<AggregateWindow<S>> scottyStratifiedSynopsis(DataStream<T> inputStream, Window[] windows, int partitionField, Class<S> synopsisClass, Object... parameters) {
        return scottyStratifiedSynopsis(inputStream, windows, partitionField, -1, synopsisClass, parameters);
    }

    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<AggregateWindow<S>> scottyWindows(DataStream<T> inputStream, Window[] windows, Class<S> synopsisClass, Object... parameters) {
        if (SamplerWithTimestamps.class.isAssignableFrom(synopsisClass)) {
            KeyedStream<Tuple2<Integer, SampleElement>, Tuple> keyedStream = inputStream.process(new ConvertToSample<>(-1)).map(new AddParallelismIndex<>()).keyBy(0);
            KeyedScottyWindowOperator<Tuple, Tuple2<Integer, SampleElement>, S> processingFunction =
                    new KeyedScottyWindowOperator<>(new SynopsisFunction(synopsisClass, parameters));
            for (int i = 0; i < windows.length; i++) {
                processingFunction.addWindow(windows[i]);
            }
            return keyedStream.process(processingFunction)
                    .flatMap(new MergePreAggregates())
                    .setParallelism(1);
        } else {
            KeyedStream<Tuple2<Integer, T>, Tuple> keyedStream = inputStream.map(new AddParallelismIndex<>()).keyBy(0);
            KeyedScottyWindowOperator<Tuple, Tuple2<Integer, T>, S> processingFunction;
            if (InvertibleSynopsis.class.isAssignableFrom(synopsisClass)) {
                processingFunction =
                        new KeyedScottyWindowOperator<>(new InvertibleSynopsisFunction(synopsisClass, parameters));
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


    /**
     * Integer state for Stateful Functions
     */
    public static class IntegerState implements ValueState<Integer> {
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

    /**
     * Stateful map functions to add the parallelism variable
     *
     * @param <T0> type of input elements
     */
    public static class AddParallelismIndex<T0> extends RichMapFunction<T0, Tuple2<Integer, T0>> {

        ValueState<Integer> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            if (parallelismKeys < 1) {
                setParallelismKeys(this.getRuntimeContext().getNumberOfParallelSubtasks());
            }
            state = new IntegerState();
        }

        @Override
        public Tuple2<Integer, T0> map(T0 value) throws Exception {
            Tuple2 newTuple = new Tuple2<Integer, T0>();
            int currentNode = state.value();
            int next = currentNode + 1;
            next = next % parallelismKeys;
            state.update(next);

            newTuple.setField(currentNode, 0);
            newTuple.setField(value, 1);

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
