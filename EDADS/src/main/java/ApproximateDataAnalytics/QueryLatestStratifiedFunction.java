package ApproximateDataAnalytics;

import Synopsis.Synopsis;
import Synopsis.WindowedSynopsis;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Consumer;
import Synopsis.StratifiedSynopsis;

public class QueryLatestStratifiedFunction<P extends Serializable, Q extends Serializable, S extends Synopsis, O extends Serializable> extends KeyedBroadcastProcessFunction<P, Tuple2<P,Q>, WindowedSynopsis<S>, StratifiedQueryResult<Q, O, P>> {

    HashMap<P, ArrayList<Tuple2<P,Q>>> queryList;
    QueryFunction<Q, S, O> queryFunction;
    Class<P> partitionClass;

    private final MapStateDescriptor<P, WindowedSynopsis<S>> synopsisMapStateDescriptor;

//    ArrayList<StratifiedQuery<Q, P>> queryList = new ArrayList<>();


    public QueryLatestStratifiedFunction(QueryFunction<Q, S, O> queryFunction, Class<P> partitionClass) {
        this.queryFunction = queryFunction;
        queryList = new HashMap<>();
        this.partitionClass = partitionClass;
        synopsisMapStateDescriptor = new MapStateDescriptor<P, WindowedSynopsis<S>>(
                "latestSynopsis",
                TypeInformation.of(partitionClass),
                TypeInformation.of(new TypeHint<WindowedSynopsis<S>>() {}));
    }


    @Override
    public void processElement(Tuple2<P,Q> query, ReadOnlyContext ctx, Collector<StratifiedQueryResult<Q, O, P>> out) throws Exception {
        ReadOnlyBroadcastState<P, WindowedSynopsis<S>> broadcastState = ctx.getBroadcastState(synopsisMapStateDescriptor);

        if (broadcastState.contains(query.f0)) {
            WindowedSynopsis<S> windowedSynopsis = broadcastState.get(query.f0);
            O queryResult = queryFunction.query(query.f1, windowedSynopsis.getSynopsis());
            out.collect(new StratifiedQueryResult<Q, O, P>(queryResult, query, windowedSynopsis));
        } else if (queryList.containsKey(query.f0)){
            queryList.get(query.f0).add(query);
        } else {
            ArrayList<Tuple2<P,Q>> queries = new ArrayList<>();
            queries.add(query);
            queryList.put(query.f0, queries);
        }
    }

    @Override
    public void processBroadcastElement(WindowedSynopsis<S> synopsis, Context ctx, Collector<StratifiedQueryResult<Q, O, P>> out) throws Exception {
        if (!(synopsis.getSynopsis() instanceof StratifiedSynopsis)){
            throw new IllegalArgumentException("The incoming synopses must be from type StratifiedSynopsis, otherwise use the ApproximateDataAnalytics.queryLatest() function to consume from a stream of synopses.");
        }
        final P partitionValue = (P) ((StratifiedSynopsis) synopsis.getSynopsis()).getPartitionValue();

        if (!ctx.getBroadcastState(synopsisMapStateDescriptor).contains(partitionValue) &&
                queryList.containsKey(partitionValue)) {
            queryList.get(partitionValue).forEach(new Consumer<Tuple2<P,Q>>() {
                @Override
                public void accept(Tuple2<P,Q> query) {
                    O queryResult = queryFunction.query(query.f1, synopsis.getSynopsis());
                    out.collect(new StratifiedQueryResult<Q, O, P>(queryResult, query, synopsis));
                }
            });
            queryList.remove(partitionValue);
        }
        ctx.getBroadcastState(synopsisMapStateDescriptor).put(partitionValue, synopsis);
    }
}
