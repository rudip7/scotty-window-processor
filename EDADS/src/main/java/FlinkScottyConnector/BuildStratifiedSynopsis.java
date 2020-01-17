package FlinkScottyConnector;

import Synopsis.InvertibleSynopsis;
import Synopsis.MergeableSynopsis;
import Synopsis.Sampling.SampleElement;
import Synopsis.Sampling.SamplerWithTimestamps;
import Synopsis.StratifiedSynopsis;
import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.windowType.Window;
import de.tub.dima.scotty.flinkconnector.KeyedScottyWindowOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * Class to organize the static functions to generate window based Synopses.
 *
 * @author Rudi Poepsel Lemaitre
 */
public final class BuildStratifiedSynopsis {

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
    public static <T extends Tuple, S extends StratifiedSynopsis & MergeableSynopsis> SingleOutputStreamOperator<S> timeBased(DataStream<T> inputStream, Time windowTime, int partitionField, int keyField, Class<S> synopsisClass, Object... parameters) {
        if (partitionField > inputStream.getType().getArity() || partitionField < 0) {
            throw new IllegalArgumentException("Partition field to execute the stratified sampling is not valid.");
        }
        if (SamplerWithTimestamps.class.isAssignableFrom(synopsisClass)) {
            return sampleTimeBased(inputStream, windowTime, partitionField, keyField, synopsisClass, parameters);
        }
        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters, partitionField, keyField);

        SingleOutputStreamOperator reduce = inputStream
                .keyBy(partitionField)
                .timeWindow(windowTime)
                .aggregate(agg)
                .returns(synopsisClass);
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
    public static <T extends Tuple, S extends StratifiedSynopsis & MergeableSynopsis> SingleOutputStreamOperator<S> slidingTimeBased(DataStream<T> inputStream, Time windowTime, Time slideTime, int partionField, int keyField, Class<S> synopsisClass, Object... parameters) {
        if (SamplerWithTimestamps.class.isAssignableFrom(synopsisClass)) {
            return slidingSampleTimeBased(inputStream, windowTime, slideTime, partionField, keyField, synopsisClass, parameters);
        }
        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters, partionField, keyField);

        SingleOutputStreamOperator reduce = inputStream
                .keyBy(partionField)
                .timeWindow(windowTime, slideTime)
                .aggregate(agg)
                .returns(synopsisClass);
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
    public static <T extends Tuple, S extends StratifiedSynopsis & MergeableSynopsis> SingleOutputStreamOperator<S> timeBased(DataStream<T> inputStream, Time windowTime, int partitionField, Class<S> synopsisClass, Object... parameters) {
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
    public static <T extends Tuple, S extends StratifiedSynopsis & MergeableSynopsis> SingleOutputStreamOperator<S> countBased(DataStream<T> inputStream, long windowSize, int partitionField, int keyField, Class<S> synopsisClass, Object... parameters) {
        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters, partitionField, keyField);
        SingleOutputStreamOperator reduce = inputStream
                .keyBy(partitionField)
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
     * @param synopsisClass the type of MergeableSynopsis to be computed
     * @param parameters    the initialization parameters for the MergeableSynopsis
     * @param <T>           the type of the input elements
     * @param <S>           the type of the MergeableSynopsis
     * @return stream of count window based Synopses
     */
    public static <T extends Tuple, S extends StratifiedSynopsis & MergeableSynopsis> SingleOutputStreamOperator<S> countBased(DataStream<T> inputStream, long windowSize, int partitionField, Class<S> synopsisClass, Object... parameters) {
        return countBased(inputStream, windowSize, partitionField, -1, synopsisClass, parameters);
    }

    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> sampleTimeBased(DataStream<T> inputStream, Time windowTime, int partitionField, int keyField, Class<S> synopsisClass, Object... parameters) {
        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters, partitionField, keyField);
        SingleOutputStreamOperator reduce = inputStream
                .process(new ConvertToSample(partitionField, keyField))
                .assignTimestampsAndWatermarks(new SampleTimeStampExtractor())
                .keyBy(0)
                .timeWindow(windowTime)
                .aggregate(agg)
                .returns(synopsisClass);
        return reduce;
    }

    public static <T, S extends MergeableSynopsis> SingleOutputStreamOperator<S> slidingSampleTimeBased(DataStream<T> inputStream, Time windowTime, Time slideTime, int partitionField, int keyField, Class<S> synopsisClass, Object... parameters) {
        SynopsisAggregator agg = new SynopsisAggregator(synopsisClass, parameters, keyField);
        SingleOutputStreamOperator reduce = inputStream
                .process(new ConvertToSample(partitionField,keyField))
                .assignTimestampsAndWatermarks(new SampleTimeStampExtractor())
                .keyBy(0)
                .timeWindow(windowTime, slideTime)
                .aggregate(agg)
                .returns(synopsisClass);
        return reduce;
    }


    public static <T extends Tuple, S extends MergeableSynopsis> SingleOutputStreamOperator<AggregateWindow<S>> scottyWindows(DataStream<T> inputStream, Window[] windows, int partitionField, int keyField, Class<S> synopsisClass, Object... parameters) {
        if (partitionField > inputStream.getType().getArity() || partitionField < 0) {
            throw new IllegalArgumentException("Partition field to execute the stratified sampling is not valid.");
        }
        if (SamplerWithTimestamps.class.isAssignableFrom(synopsisClass)) {
            KeyedStream<Tuple2<String, SampleElement>, Tuple> keyedStream = inputStream.process(new ConvertToSample<>(partitionField, keyField)).keyBy(0);
            KeyedScottyWindowOperator<Tuple, Tuple2<String, SampleElement>, S> processingFunction =
                    new KeyedScottyWindowOperator<>(new SynopsisFunction(1, partitionField, synopsisClass, parameters));
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

    public static <T extends Tuple, S extends MergeableSynopsis> SingleOutputStreamOperator<AggregateWindow<S>> scottyWindows(DataStream<T> inputStream, Window[] windows, int partitionField, Class<S> synopsisClass, Object... parameters) {
        return scottyWindows(inputStream, windows, partitionField, -1, synopsisClass, parameters);
    }




    public static class ConvertToSample<T extends Tuple>
            extends ProcessFunction<T, Tuple2<String, SampleElement>> {
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
        public void processElement(T value, Context ctx, Collector<Tuple2<String, SampleElement>> out) throws Exception {
            if (keyField >= 0) {
                SampleElement sample = new SampleElement<>(value.getField(keyField), ctx.timestamp() != null ? ctx.timestamp() : ctx.timerService().currentProcessingTime());
                out.collect(new Tuple2(value.getField(partitionField).toString(), sample));
            } else {
                SampleElement<T> sample = new SampleElement<>(value, ctx.timestamp() != null ? ctx.timestamp() : ctx.timerService().currentProcessingTime());
                out.collect(new Tuple2(value.getField(partitionField).toString(), sample));
            }
        }
    }

    /**
     * The Custom TimeStampExtractor which is used to assign Timestamps and Watermarks for our data
     */
    public static class SampleTimeStampExtractor implements AssignerWithPunctuatedWatermarks<Tuple2<Object, SampleElement>> {
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
        public Watermark checkAndGetNextWatermark(Tuple2<Object, SampleElement> lastElement, long extractedTimestamp) {
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
        public long extractTimestamp(Tuple2<Object, SampleElement> element, long previousElementTimestamp) {
            return element.f1.getTimeStamp();
        }
    }
}