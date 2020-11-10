package Benchmark;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class RichStratifier extends RichMapFunction<Tuple3<Integer, Integer, Long>, Tuple2<Integer, Integer>> {

    private int stratification;
    private boolean isUniform;

    public RichStratifier(int stratification, boolean isUniform) {
        this.stratification = stratification;
        this.isUniform = isUniform;
    }

    @Override
    public Tuple2<Integer, Integer> map(Tuple3<Integer, Integer, Long> value) throws Exception {
        if(isUniform){
            int key = value.f0/1000;
            return new Tuple2<>(key, value.f0);
        } else {
            if (value.f0 < stratification){
                return new Tuple2<>(value.f0, value.f0);
            } else {
                return new Tuple2<>(stratification-1, value.f0);
            }
        }
    }
}
