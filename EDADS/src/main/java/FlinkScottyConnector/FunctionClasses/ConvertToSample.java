package FlinkScottyConnector.FunctionClasses;

import Synopsis.Sampling.TimestampedElement;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Wrap an Element into a TimestampedElement
 * @param <T>
 */
public class ConvertToSample<T> extends ProcessFunction<T, Object> {
    @Override
    public void processElement(T value, Context ctx, Collector<Object> out) throws Exception {
        TimestampedElement<T> sample = new TimestampedElement<>(value, ctx.timestamp() != null ? ctx.timestamp() : ctx.timerService().currentProcessingTime());
        out.collect(sample);
    }
}
