package StreamApprox;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import scala.Int;

public class RichStratifier extends RichMapFunction<Tuple3<Integer, Integer, Long>, Tuple2<Integer, Integer>> {

    private int stratification;

    @Override
    public Tuple2<Integer, Integer> map(Tuple3<Integer, Integer, Long> value) throws Exception {
        int key = (int)(value.f0 / 100d * stratification);
        if (key >= stratification){
            key = stratification -1;
        }
        return new Tuple2<>(key, value.f0);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ExecutionConfig.GlobalJobParameters globalParams = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        Configuration globConf = (Configuration) globalParams;
        stratification = globConf.getInteger("stratification", 1);
    }
}