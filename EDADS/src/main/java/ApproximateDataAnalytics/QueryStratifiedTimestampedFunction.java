package ApproximateDataAnalytics;

import Synopsis.Synopsis;
import Synopsis.StratifiedSynopsis;
import Synopsis.WindowedSynopsis;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import java.io.Serializable;
import java.util.*;

/**
 * KeyedBroadcastFunction which combines a keyed stream of queries with a Stream of Synopsis which are based on the same keys.
 * The queries are timestamped and if a synopsis with the correct key and window time exists will be answered, otherwise dismissed.
 *
 * @author Joscha von Hein
 * @param <P>   Partition / Key Type
 * @param <Q>   Query Type
 * @param <S>   Synopsis Type
 * @param <O>   Query Result Type
 */
public class QueryStratifiedTimestampedFunction<P extends Serializable, Q extends Serializable, S extends StratifiedSynopsis<P> & Synopsis, O extends Serializable> extends KeyedBroadcastProcessFunction<P, Tuple2<P, TimestampedQuery<Q>>, WindowedSynopsis<S>, QueryResult<TimestampedQuery<Q>, O>> {

    final int maxSynopsisCount; // maximum synopsis count per strata / key
    final QueryFunction<TimestampedQuery<Q>, WindowedSynopsis<S>, QueryResult<TimestampedQuery<Q>, O>> queryFunction;
    final MapStateDescriptor<P, TreeSet<WindowedSynopsis<S>>> synopsisMapStateDescriptor;
    HashMap<P, LinkedList<TimestampedQuery<Q>>> queryHashMap = new HashMap<P, LinkedList<TimestampedQuery<Q>>>();

    public QueryStratifiedTimestampedFunction(int maxSynopsisCount,
                                              QueryFunction<TimestampedQuery<Q>, WindowedSynopsis<S>, QueryResult<TimestampedQuery<Q>, O>> queryFunction,
                                              MapStateDescriptor<P, TreeSet<WindowedSynopsis<S>>> synopsisMapStateDescriptor) {
        this.maxSynopsisCount = maxSynopsisCount;
        this.queryFunction = queryFunction;
        this.synopsisMapStateDescriptor = synopsisMapStateDescriptor;
    }

    @Override
    public void processElement(Tuple2<P, TimestampedQuery<Q>> value, ReadOnlyContext ctx, Collector<QueryResult<TimestampedQuery<Q>, O>> out) throws Exception {
        P key = ctx.getCurrentKey();
        if (ctx.getBroadcastState(synopsisMapStateDescriptor).contains(key)){

            WindowedSynopsis<S> querySynopsis = ctx.getBroadcastState(synopsisMapStateDescriptor).get(key)
                    .floor(new WindowedSynopsis<S>(null, value.f1.getTimeStamp(), Long.MAX_VALUE));

            if (querySynopsis != null && querySynopsis.getWindowEnd() >= value.f1.getTimeStamp()){ // synopsis with correct window exists

                out.collect(queryFunction.query(value.f1, querySynopsis));
            }
        }else {
            // store the query
            LinkedList<TimestampedQuery<Q>> map;
            if (queryHashMap.containsKey(key)){
                map = queryHashMap.get(key);
            } else {
                map = new LinkedList<TimestampedQuery<Q>>();
            }
            map.add(value.f1);
            queryHashMap.put(key, map);
        }
    }

    @Override
    public void processBroadcastElement(WindowedSynopsis<S> value, Context ctx, Collector<QueryResult<TimestampedQuery<Q>, O>> out) throws Exception {
        TreeSet<WindowedSynopsis<S>> windowedSynopses;
        P key = value.getSynopsis().getPartitionValue();
        if (ctx.getBroadcastState(synopsisMapStateDescriptor).contains(key)){
            windowedSynopses = ctx.getBroadcastState(synopsisMapStateDescriptor).get(key);
            if (windowedSynopses.size() >= maxSynopsisCount){
                windowedSynopses.pollFirst();
            }
            windowedSynopses.add(value);
            ctx.getBroadcastState(synopsisMapStateDescriptor).put(key, windowedSynopses);
        } else {
            windowedSynopses = new TreeSet<WindowedSynopsis<S>>(new Comparator<WindowedSynopsis<S>>() {
                @Override
                public int compare(WindowedSynopsis<S> o1, WindowedSynopsis<S> o2) {
                    return Long.compare(o1.getWindowStart(), o2.getWindowStart());
                }
            });
            windowedSynopses.add(value);
            ctx.getBroadcastState(synopsisMapStateDescriptor).put(key, windowedSynopses);
            if (queryHashMap.containsKey(key)){
                queryHashMap.get(key).stream()
                        .filter(query -> query.getTimeStamp() >= value.getWindowStart() && query.getTimeStamp() <= value.getWindowEnd())
                        .forEach(query -> out.collect(queryFunction.query(query, value)));
            }
        }
    }
}
