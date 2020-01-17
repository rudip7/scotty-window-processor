package FlinkScottyConnector;

import Synopsis.MergeableSynopsis;
import Synopsis.StratifiedSynopsis;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectStreamException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * General {@link AggregateFunction} to build a customized MergeableSynopsis in an incremental way.
 *
 * @param <T1>
 * @author Rudi Poepsel Lemaitre
 */
public class SynopsisAggregator<T1> implements AggregateFunction<T1, MergeableSynopsis, MergeableSynopsis> {

    private static final Logger LOG = LoggerFactory.getLogger(LocalStreamEnvironment.class);
    private int keyField;
    private int partitionField;
    private Class<? extends MergeableSynopsis> sketchClass;
    private Object[] constructorParam;

    /**
     * Construct a new MergeableSynopsis Aggregator Function.
     *
     * @param sketchClass the MergeableSynopsis.class
     * @param params      The parameters of the MergeableSynopsis as an Object array
     * @param keyField    The keyField with which to update the MergeableSynopsis. To update with the whole Tuple use -1!
     */
    public SynopsisAggregator(Class<? extends MergeableSynopsis> sketchClass, Object[] params, int keyField) {
        this.keyField = keyField;
        this.sketchClass = sketchClass;
        this.constructorParam = params;
        this.partitionField = -1;
    }

    /**
     * Construct a new MergeableSynopsis Aggregator Function.
     *
     * @param sketchClass the MergeableSynopsis.class
     * @param params      The parameters of the MergeableSynopsis as an Object array
     * @param keyField    The keyField with which to update the MergeableSynopsis. To update with the whole Tuple use -1!
     */
    public SynopsisAggregator(Class<? extends MergeableSynopsis> sketchClass, Object[] params, int partitionField, int keyField) {
        this.keyField = keyField;
        this.partitionField = partitionField;
        this.sketchClass = sketchClass;
        this.constructorParam = params;
    }

    /**
     * Creates a new MergeableSynopsis (accumulator), starting a new aggregate.
     * The accumulator is the state of a running aggregation. When a program has multiple
     * aggregates in progress (such as per key and window), the state (per key and window)
     * is the size of the accumulator.
     *
     * @return A new MergeableSynopsis (accumulator), corresponding to an empty MergeableSynopsis.
     */
    @Override
    public MergeableSynopsis createAccumulator() {
        Class<?>[] parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
        try {
            Constructor<? extends MergeableSynopsis> constructor = sketchClass.getConstructor(parameterClasses);
            MergeableSynopsis synopsis = constructor.newInstance(constructorParam);
            return synopsis;
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("There is no constructor in class " + sketchClass + " that match with the given parameters.");
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        throw new IllegalArgumentException();
    }

    /**
     * Updates the MergeableSynopsis structure by the given input value, returning the
     * new accumulator value.
     * <p>
     * For efficiency, the input accumulator is modified and returned.
     *
     * @param value       The value to add
     * @param accumulator The MergeableSynopsis to add the value to
     */
    @Override
    public MergeableSynopsis add(T1 value, MergeableSynopsis accumulator) {
        if (partitionField < 0) {
            if (!(value instanceof Tuple2)) {
                throw new IllegalArgumentException("Incoming elements must be from type to build a synopsis.");
            }
            Tuple2 tupleValue = (Tuple2) value;
            if (tupleValue.f1 instanceof Tuple && keyField != -1) {
                Object field = ((Tuple) tupleValue.f1).getField(this.keyField);
                accumulator.update(field);
                return accumulator;
            }
            accumulator.update(tupleValue.f1);
            return accumulator;
        } else {
            if (!(value instanceof Tuple)) {
                throw new IllegalArgumentException("Incoming elements must be from type Tuple to build a stratified synopsis.");
            }
            Tuple tupleValue = (Tuple) value;

            ((StratifiedSynopsis) accumulator).setPartitionValue(tupleValue.getField(this.partitionField));
            if (keyField != -1) {
                Object field = tupleValue.getField(this.keyField);
                accumulator.update(field);
                return accumulator;
            }
            accumulator.update(tupleValue);
            return accumulator;

        }
    }

    /**
     * Gets the result of the aggregation from the accumulator.
     *
     * @param accumulator The accumulator of the aggregation
     * @return The final aggregation result.
     */
    @Override
    public MergeableSynopsis getResult(MergeableSynopsis accumulator) {
        return accumulator;
    }

    /**
     * Merges two accumulators, returning an accumulator with the merged state.
     * <p>
     * This function may reuse any of the given accumulators as the target for the merge
     * and return that. The assumption is that the given accumulators will not be used any
     * more after having been passed to this function.
     *
     * @param a An accumulator to merge
     * @param b Another accumulator to merge
     * @return The accumulator with the merged state
     */
    @Override
    public MergeableSynopsis merge(MergeableSynopsis a, MergeableSynopsis b) {
        try {
            return a.merge(b);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeInt(keyField);
        out.writeObject(constructorParam);
        out.writeObject(sketchClass);
        out.writeInt(partitionField);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        keyField = in.readInt();
        constructorParam = (Object[]) in.readObject();
        sketchClass = (Class<? extends MergeableSynopsis>) in.readObject();
        partitionField = in.readInt();
    }

    private void readObjectNoData() throws ObjectStreamException {
        throw new NotSerializableException("Serialization error in class " + this.getClass().getName());
    }
}


