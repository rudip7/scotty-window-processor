package ApproximateDataAnalytics;

import Synopsis.Synopsis;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.function.Consumer;

/**
 * Class which enables querying a stream of Synopsis easily for the user
 *
 * @author Joscha von Hein
 */
public final class ApproximateDataAnalytics {

    public <Q, O, S extends Synopsis> DataStream<O> queryLatest(DataStream<S> sketchStream, DataStream<Q> queryStream, QueryFunction<S, Q, O> queryFunction, Class<O> clazz){

        CustomBroadcastProcessFunction<Q, O, S> broadcastProcessFunction = new CustomBroadcastProcessFunction<Q, O, S>(queryFunction);

        DataStream<S> broadcast = sketchStream.broadcast();


        SingleOutputStreamOperator<O> process = queryStream.connect(broadcast)
                .process(new CustomBroadcastProcessFunction<Q, O, S>(queryFunction))
                .returns(clazz);

    }


    private static class CustomBroadcastProcessFunction<Q, O, S extends Synopsis> extends BroadcastProcessFunction<Q, S, O>{

        private final MapStateDescriptor<Boolean, S> synopsisMapStateDescriptor= new MapStateDescriptor<Boolean, S>(
                "latest Synopsis",
                BasicTypeInfo.BOOLEAN_TYPE_INFO,
                TypeInformation.of(new TypeHint<S>() {}));

        ArrayList<Q> queryList = new ArrayList<>();
        QueryFunction<S, Q, O> queryFunction;

        public CustomBroadcastProcessFunction(QueryFunction<S, Q, O> queryFunction){
            this.queryFunction = queryFunction;
        }

        @Override
        public void processElement(Q query, ReadOnlyContext ctx, Collector<O> out) throws Exception {
            ReadOnlyBroadcastState<Boolean, S> broadcastState = ctx.getBroadcastState(synopsisMapStateDescriptor);

            if (broadcastState.contains(true)){
                out.collect(queryFunction.query(broadcastState.get(true), query));
            } else {
                queryList.add(query);
            }
        }

        @Override
        public void processBroadcastElement(S sketch, Context ctx, Collector<O> out) throws Exception {
            if (ctx.getBroadcastState(synopsisMapStateDescriptor).contains(true) && queryList.size() > 0) {
                queryList.forEach(new Consumer<Q>() {
                    @Override
                    public void accept(Q q) {
                        out.collect(queryFunction.query(sketch, q));
                    }
                });
            }
            ctx.getBroadcastState(synopsisMapStateDescriptor).put(true, sketch);
        }
    }

}
