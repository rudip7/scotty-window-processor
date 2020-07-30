package ApproximateDataAnalytics;

import Synopsis.StratifiedSynopsisWrapper;
import Synopsis.Synopsis;
import Synopsis.WindowedSynopsis;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Consumer;

public class QueryLatestStratifiedFunction<P extends Serializable, Q extends Serializable, S extends Synopsis, O extends Serializable> extends KeyedBroadcastProcessFunction<P, Tuple2<P,Q>, StratifiedSynopsisWrapper<P, WindowedSynopsis<S>>, StratifiedQueryResult<Q, O, P>> {

    HashMap<P, ArrayList<Tuple2<P,Q>>> queryList;
    QueryFunction<Q, S, O> queryFunction;
    Class<P> partitionClass;

    private final MapStateDescriptor<P, WindowedSynopsis<S>> synopsisMapStateDescriptor;


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
    public void processBroadcastElement(StratifiedSynopsisWrapper<P, WindowedSynopsis<S>> stratifiedSynopsisWrapper, Context ctx, Collector<StratifiedQueryResult<Q, O, P>> out) throws Exception {

        final P key = stratifiedSynopsisWrapper.getKey();
        final WindowedSynopsis<S> windowedSynopsis = stratifiedSynopsisWrapper.getSynopsis();

        if (!ctx.getBroadcastState(synopsisMapStateDescriptor).contains(key) &&
                queryList.containsKey(key)) {

            queryList.get(key).forEach(new Consumer<Tuple2<P,Q>>() {
                @Override
                public void accept(Tuple2<P,Q> query) {
                    O queryResult = queryFunction.query(query.f1, windowedSynopsis.getSynopsis());
                    out.collect(new StratifiedQueryResult<Q, O, P>(queryResult, query, windowedSynopsis));
                }
            });
            queryList.remove(key);
        }
        ctx.getBroadcastState(synopsisMapStateDescriptor).put(key, windowedSynopsis);
    }
}
