package ApproximateDataAnalytics;

import Synopsis.Sampling.TimestampedElement;
import Synopsis.Synopsis;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.function.Consumer;

import Synopsis.WindowedSynopsis;
import Synopsis.StratifiedSynopsis;

/**
 * Class which enables querying a stream of Synopsis easily for the user
 *
 * @author Joscha von Hein
 * @author Rudi Poepsel Lemaitre
 */
public final class ApproximateDataAnalytics {

    public static <Q extends Serializable, S extends Synopsis, O extends Serializable> SingleOutputStreamOperator<QueryResult<Q, O>> queryLatest(DataStream<WindowedSynopsis<S>> synopsesStream, DataStream<Q> queryStream, QueryFunction<Q, S, O> queryFunction) {
        MapStateDescriptor<Boolean, WindowedSynopsis<S>> synopsisMapStateDescriptor = new MapStateDescriptor<Boolean, WindowedSynopsis<S>>(
                "latestSynopsis",
                BasicTypeInfo.BOOLEAN_TYPE_INFO,
                TypeInformation.of(new TypeHint<WindowedSynopsis<S>>() {
                }));

        BroadcastStream<WindowedSynopsis<S>> broadcast = synopsesStream.broadcast(synopsisMapStateDescriptor);
        return queryStream.connect(broadcast)
                .process(new QueryLatestFunction<Q, S, O>(queryFunction));
    }

    public static <Q extends Serializable, S extends Synopsis, O extends Serializable> SingleOutputStreamOperator<QueryResult<Q, O>> queryTimestamped(DataStream<WindowedSynopsis<S>> synopsesStream, DataStream<TimestampedQuery<Q>> queryStream, QueryFunction<Q, S, O> queryFunction) {
        //TODO Joscha
        return null;
    }


    public static <P extends Serializable, Q extends Serializable, S extends Synopsis, O extends Serializable> SingleOutputStreamOperator<StratifiedQueryResult<Q, O, P>> queryLatestStratified(DataStream<WindowedSynopsis<S>> synopsesStream, DataStream<Tuple2<P, Q>> queryStream, QueryFunction<Q, S, O> queryFunction, Class<P> partitionClass) {
        MapStateDescriptor<P, WindowedSynopsis<S>> synopsisMapStateDescriptor = new MapStateDescriptor<P, WindowedSynopsis<S>>(
                "latestSynopsis",
                TypeInformation.of(partitionClass),
                TypeInformation.of(new TypeHint<WindowedSynopsis<S>>() {
                }));

        BroadcastStream<WindowedSynopsis<S>> broadcast = synopsesStream.broadcast(synopsisMapStateDescriptor);

        KeyedStream<Tuple2<P, Q>, Tuple> keyedQueryStream = queryStream.keyBy(0);

        return keyedQueryStream.connect(broadcast)
                .process(new QueryLatestStratifiedFunction<P, Q, S, O>(queryFunction, partitionClass));
    }

    public static <P extends Serializable, Q extends Serializable, S extends Synopsis, O extends Serializable> SingleOutputStreamOperator<QueryResult<Q, O>> queryTimestampedStratified(DataStream<WindowedSynopsis<S>> synopsesStream, DataStream<Tuple2<P, TimestampedQuery<Q>>> queryStream, QueryFunction<Q, S, O> queryFunction) {
        //TODO Joscha
        return null;
    }


}
