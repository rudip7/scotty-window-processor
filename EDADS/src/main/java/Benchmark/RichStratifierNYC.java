package Benchmark;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple2;

public class RichStratifierNYC extends RichMapFunction<Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short>, Tuple2<Integer, Long>> {

    private int stratification;

    public RichStratifierNYC(int stratification) {
        this.stratification = stratification;
    }

    @Override
    public Tuple2<Integer, Long> map(Tuple11<Long, Long, Long, Boolean, Long, Long, Float, Float, Float, Float, Short> value) throws Exception {
        int key = (int) (value.f2 % stratification);
        return new Tuple2<>(key, value.f0);
    }
}
