package FlinkScottyConnector;

import Synopsis.*;
import Synopsis.Sampling.TimestampedElement;
import Synopsis.Sampling.SamplerWithTimestamps;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.flinkconnector.KeyedScottyWindowOperator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.PriorityQueue;

/**
 * Class to organize the static functions to generate window based Synopses.
 *
 * @author Rudi Poepsel Lemaitre
 */
public final class BuildStratifiedSynopsis {

    private static int parallelismKeys = -1;

    public static void setParallelismKeys(int newParallelismKeys) {
        parallelismKeys = newParallelismKeys;
    }

    public static <T, S extends Synopsis> SingleOutputStreamOperator<S> timeBased(DataStream<T> inputStream, int miniBatchSize, Time windowTime, Time slideTime, int keyField, Class<S> synopsisClass, Object... parameters) {
        NonMergeableSynopsisAggregator agg = new NonMergeableSynopsisAggregator(synopsisClass, parameters, keyField);
        KeyedStream keyBy = inputStream
                .process(new OrderAndIndex(keyField, miniBatchSize)).setParallelism(1)
                .keyBy(0);

        WindowedStream windowedStream;
        if (slideTime == null) {
            windowedStream = keyBy.timeWindow(windowTime);
        } else {
            windowedStream = keyBy.timeWindow(windowTime, slideTime);
        }

        return windowedStream
                .aggregate(agg).returns(synopsisClass);
    }

    public static <T, S extends Synopsis> SingleOutputStreamOperator<S> timeBased(DataStream<T> inputStream, Time windowTime, Time slideTime, Class<S> synopsisClass, Object... parameters) {
        return timeBased(inputStream, 0, windowTime, slideTime, -1, synopsisClass, parameters);
    }

    public static <T, S extends Synopsis> SingleOutputStreamOperator<S> timeBased(DataStream<T> inputStream, int miniBatchSize, Time windowTime, Time slideTime, Class<S> synopsisClass, Object... parameters) {
        return timeBased(inputStream, miniBatchSize, windowTime, slideTime, -1, synopsisClass, parameters);
    }

    public static <T, S extends Synopsis> SingleOutputStreamOperator<S> timeBased(DataStream<T> inputStream, Time windowTime, Time slideTime, int keyField, Class<S> synopsisClass, Object... parameters) {
        return timeBased(inputStream, 0, windowTime, slideTime, keyField, synopsisClass, parameters);
    }

    public static <T, S extends Synopsis> SingleOutputStreamOperator<S> timeBased(DataStream<T> inputStream, Time windowTime, Class<S> synopsisClass, Object... parameters) {
        return timeBased(inputStream, 0, windowTime, null, -1, synopsisClass, parameters);
    }


    public static <T, S extends Synopsis, SM extends NonMergeableSynopsisManager> SingleOutputStreamOperator<AggregateWindow<SM>> scottyWindows(DataStream<T> inputStream, int miniBatchSize, Window[] windows, int keyField, Class<S> synopsisClass, Class<SM> sliceManagerClass, Object... parameters) {

        KeyedStream<Tuple2<Integer, Object>, Tuple> keyedStream = inputStream
                .process(new OrderAndIndex(keyField, miniBatchSize)).setParallelism(1)
                .keyBy(0);

        KeyedScottyWindowOperator<Tuple, Tuple2<Integer, Object>, SM> processingFunction =
                new KeyedScottyWindowOperator<>(new NonMergeableSynopsisFunction(keyField, -1, synopsisClass, sliceManagerClass, parameters));

        for (int i = 0; i < windows.length; i++) {
            processingFunction.addWindow(windows[i]);
        }
        return keyedStream.process(processingFunction);
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
    public static <T extends Tuple, S extends MergeableSynopsis> SingleOutputStreamOperator<S> timeBased(DataStream<T> inputStream, Time windowTime, int partitionField, int keyField, Class<S> synopsisClass, Object... parameters) {
        return timeBased(inputStream,windowTime,null,partitionField,keyField,synopsisClass,parameters);
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
    public static <T extends Tuple, S extends MergeableSynopsis> SingleOutputStreamOperator<S> timeBased(DataStream<T> inputStream, Time windowTime, Time slideTime, int partitionField, int keyField, Class<S> synopsisClass, Object... parameters) {
        if (!StratifiedSynopsis.class.isAssignableFrom(synopsisClass)) {
            throw new IllegalArgumentException("Synopsis class needs to extend the StratifiedSynopsis abstract class to build a stratified synopsis.");
        }
        if (partitionField > inputStream.getType().getArity() || partitionField < 0) {
            throw new IllegalArgumentException("Partition field to execute the stratified sampling is not valid.");
        }
        SynopsisAggregator agg = new SynopsisAggregator(true, synopsisClass, parameters);

        KeyedStream keyBy;

        if (SamplerWithTimestamps.class.isAssignableFrom(synopsisClass)) {
            keyBy = inputStream
                    .process(new ConvertToSample(partitionField, keyField))
                    .assignTimestampsAndWatermarks(new SampleTimeStampExtractor())
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

        SingleOutputStreamOperator result = windowedStream
                .aggregate(agg)
                .returns(synopsisClass);

        return result;
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
    public static <T extends Tuple, S extends MergeableSynopsis> SingleOutputStreamOperator<S> timeBased(DataStream<T> inputStream, Time windowTime, int partitionField, Class<S> synopsisClass, Object... parameters) {
        return timeBased(inputStream, windowTime, partitionField, -1, synopsisClass, parameters);
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
    public static <T extends Tuple, S extends MergeableSynopsis> SingleOutputStreamOperator<S> countBased(DataStream<T> inputStream, long windowSize, int partitionField, int keyField, Class<S> synopsisClass, Object... parameters) {
        if (!StratifiedSynopsis.class.isAssignableFrom(synopsisClass)) {
            throw new IllegalArgumentException("Synopsis class needs to extend the StratifiedSynopsis abstract class to build a stratified synopsis.");
        }
        SynopsisAggregator agg = new SynopsisAggregator(true, synopsisClass, parameters);
        SingleOutputStreamOperator reduce = inputStream
                .map(new TransformStratified<>(partitionField, keyField))
                .keyBy(0)
                .countWindow(windowSize)
                .aggregate(agg)
                .returns(synopsisClass);
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
    public static <T extends Tuple, S extends MergeableSynopsis> SingleOutputStreamOperator<S> countBased(DataStream<T> inputStream, long windowSize, long slideSize, int partitionField, int keyField, Class<S> synopsisClass, Object... parameters) {
        if (!StratifiedSynopsis.class.isAssignableFrom(synopsisClass)) {
            throw new IllegalArgumentException("Synopsis class needs to extend the StratifiedSynopsis abstract class to build a stratified synopsis.");
        }
        if (partitionField > inputStream.getType().getArity() || partitionField < 0) {
            throw new IllegalArgumentException("Partition field to execute the stratified sampling is not valid.");
        }
        SynopsisAggregator agg = new SynopsisAggregator(true, synopsisClass, parameters);

        KeyedStream keyBy;

        if (SamplerWithTimestamps.class.isAssignableFrom(synopsisClass)) {
            keyBy = inputStream
                    .process(new ConvertToSample(partitionField, keyField))
                    .assignTimestampsAndWatermarks(new SampleTimeStampExtractor())
                    .keyBy(0);
        } else {
            keyBy = inputStream
                    .map(new TransformStratified<>(partitionField, keyField))
                    .keyBy(0);
        }

        WindowedStream windowedStream;
        if (slideSize == -1) {
            windowedStream = keyBy.countWindow(windowSize);
        } else {
            windowedStream = keyBy.countWindow(windowSize, slideSize);
        }

        SingleOutputStreamOperator result = windowedStream
                .aggregate(agg)
                .returns(synopsisClass);

        return result;
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
    public static <T extends Tuple, S extends MergeableSynopsis> SingleOutputStreamOperator<S> countBased(DataStream<T> inputStream, long windowSize, int partitionField, Class<S> synopsisClass, Object... parameters) {
        return countBased(inputStream, windowSize, partitionField, -1, synopsisClass, parameters);
    }


    public static <T extends Tuple, S extends MergeableSynopsis> SingleOutputStreamOperator<AggregateWindow<S>> scottyWindows(DataStream<T> inputStream, Window[] windows, int partitionField, int keyField, Class<S> synopsisClass, Object... parameters) {
        if (!StratifiedSynopsis.class.isAssignableFrom(synopsisClass)) {
            throw new IllegalArgumentException("Synopsis class needs to extend the StratifiedSynopsis abstract class to build a stratified synopsis.");
        }
        if (partitionField > inputStream.getType().getArity() || partitionField < 0) {
            throw new IllegalArgumentException("Partition field to execute the stratified sampling is not valid.");
        }
        if (SamplerWithTimestamps.class.isAssignableFrom(synopsisClass)) {
            KeyedStream<Tuple2<String, TimestampedElement>, Tuple> keyedStream = inputStream.process(new ConvertToSample<>(partitionField, keyField)).keyBy(0);
            KeyedScottyWindowOperator<Tuple, Tuple2<String, TimestampedElement>, S> processingFunction =
                    new KeyedScottyWindowOperator<>(new SynopsisFunction(true, synopsisClass, parameters));
            for (int i = 0; i < windows.length; i++) {
                processingFunction.addWindow(windows[i]);
            }
            return keyedStream.process(processingFunction);
        } else {
            KeyedStream<Tuple2<String,Object>, Tuple> keyedStream = inputStream.map(new TransformStratified(partitionField, keyField)).keyBy(0);
            KeyedScottyWindowOperator<Tuple, Tuple2<String,Object>, S> processingFunction;
            if (InvertibleSynopsis.class.isAssignableFrom(synopsisClass)) {
                processingFunction =
                        new KeyedScottyWindowOperator<>(new InvertibleSynopsisFunction(true, synopsisClass, parameters));
            } else if (CommutativeSynopsis.class.isAssignableFrom(synopsisClass)) {
                processingFunction =
                        new KeyedScottyWindowOperator<>(new CommutativeSynopsisFunction(synopsisClass, parameters));
            } else {
                processingFunction =
                        new KeyedScottyWindowOperator<>(new SynopsisFunction(true, synopsisClass, parameters));
            }
            for (int i = 0; i < windows.length; i++) {
                processingFunction.addWindow(windows[i]);
            }
            return keyedStream.process(processingFunction);
        }

    }

    public static <T extends Tuple, S extends MergeableSynopsis> SingleOutputStreamOperator<AggregateWindow<S>> scottyWindows(DataStream<T> inputStream, Window[] windows, int partitionField, Class<S> synopsisClass, Object... parameters) {
        return scottyWindows(inputStream, windows, partitionField, -1, synopsisClass, parameters);
    }


    public static class ConvertToSample<T extends Tuple>
            extends ProcessFunction<T, Tuple2<String, TimestampedElement>> {
        private int keyField = -1;
        private int partitionField;

        public ConvertToSample(int partitionField, int keyField) {
            this.keyField = keyField;
            this.partitionField = partitionField;
        }

        public ConvertToSample(int partitionField) {
            this.partitionField = partitionField;
        }

        @Override
        public void processElement(T value, Context ctx, Collector<Tuple2<String, TimestampedElement>> out) throws Exception {
            if (keyField >= 0) {
                TimestampedElement sample = new TimestampedElement<>(value.getField(keyField), ctx.timestamp() != null ? ctx.timestamp() : ctx.timerService().currentProcessingTime());
                out.collect(new Tuple2(value.getField(partitionField).toString(), sample));
            } else {
                TimestampedElement<T> sample = new TimestampedElement<>(value, ctx.timestamp() != null ? ctx.timestamp() : ctx.timerService().currentProcessingTime());
                out.collect(new Tuple2(value.getField(partitionField).toString(), sample));
            }
        }
    }

    /**
     * The Custom TimeStampExtractor which is used to assign Timestamps and Watermarks for our data
     */
    public static class SampleTimeStampExtractor implements AssignerWithPunctuatedWatermarks<Tuple2<Object, TimestampedElement>> {
        /**
         * Asks this implementation if it wants to emit a watermark. This method is called right after
         * the    method.
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
        public Watermark checkAndGetNextWatermark(Tuple2<Object, TimestampedElement> lastElement, long extractedTimestamp) {
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
        public long extractTimestamp(Tuple2<Object, TimestampedElement> element, long previousElementTimestamp) {
            return element.f1.getTimeStamp();
        }
    }


    /**
     * Stateful map functions to add the parallelism variable
     *
     * @param <T0> type of input elements
     */
    public static class TransformStratified<T0 extends Tuple> implements MapFunction<T0, Tuple2<String, Object>> {

        public int partitionField;
        public int keyField;
        private Tuple2<String, Object> newTuple;

        public TransformStratified(int partitionField, int keyField) {
            this.keyField = keyField;
            this.partitionField = partitionField;
            newTuple = new Tuple2<>();
        }

        @Override
        public Tuple2<String, Object> map(T0 value) throws Exception {
            if (keyField != -1) {
                newTuple.setField(value.getField(keyField), 1);
            } else {
                newTuple.setField(value, 1);
            }
            newTuple.setField(value.getField(partitionField).toString(), 0);
            return newTuple;
        }
    }


    private static class OrderAndIndex<T0> extends ProcessFunction<T0, Tuple2<Integer, Object>> {
        private int keyField;
        private int miniBatchSize;

        private transient ValueState<Integer> state;
        private Tuple2<Integer, Object> newTuple;

        private PriorityQueue<TimestampedElement> dispatchList;

        public OrderAndIndex(int keyField, int miniBatchSize) {
            this.keyField = keyField;
            this.miniBatchSize = miniBatchSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            if (parallelismKeys < 1) {
                throw new IllegalArgumentException("The parallelism for the synopsis construction needs to be set with the BuildSynopsis.setParallelismKeys() method.");
            }
            state = new BuildSynopsis.IntegerState();
            if (miniBatchSize > 1) {
                dispatchList = new PriorityQueue<>();
            }
            newTuple = new Tuple2<>();
        }

        @Override
        public void processElement(T0 value, Context ctx, Collector<Tuple2<Integer, Object>> out) throws Exception {
            if (miniBatchSize > 1) {
                if (value instanceof Tuple && keyField != -1) {
                    dispatchList.add(new TimestampedElement(((Tuple) value).getField(keyField), ctx.timestamp() != null ? ctx.timestamp() : ctx.timerService().currentProcessingTime()));
                } else {
                    dispatchList.add(new TimestampedElement(value, ctx.timestamp() != null ? ctx.timestamp() : ctx.timerService().currentProcessingTime()));
                }

                if (dispatchList.size() == miniBatchSize) {
                    while (!dispatchList.isEmpty()) {
                        int currentNode = state.value();
                        int next = currentNode + 1;
                        next = next % parallelismKeys;
                        state.update(next);

                        Object tupleValue = dispatchList.poll().getValue();

                        newTuple.setField(tupleValue, 1);
                        newTuple.setField(currentNode, 0);
                        out.collect(newTuple);
                    }
                }
            } else {
                int currentNode = state.value();
                int next = currentNode + 1;
                next = next % parallelismKeys;
                state.update(next);

                if (value instanceof Tuple && keyField != -1) {
                    newTuple.setField(((Tuple) value).getField(keyField), 1);
                } else {
                    newTuple.setField(value, 1);
                }
                newTuple.setField(currentNode, 0);
                out.collect(newTuple);
            }
        }
    }
}