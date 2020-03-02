package Benchmark.FlinkBenchmarkJobs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class TemporaryMapFunction implements MapFunction {
    @Override
    public Tuple2<Object, Object> map(Object value) throws Exception {
        // TODO: this is just a temporary function which does nothing but trying to erase compile errors
        return new Tuple2<>(0, 0);
    }
}
