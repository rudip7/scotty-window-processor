package FlinkScottyConnector;

import Synopsis.MergeableSynopsis;
import Synopsis.NonMergeableSynopsis;
import Synopsis.Wavelets.SliceWaveletsManager;
import Synopsis.Wavelets.WaveletSynopsis;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class WaveletFunction<Input> implements AggregateFunction<Tuple2<Integer,Input>, WaveletSynopsis, SliceWaveletsManager>, Serializable {
    private int keyField;
    private Object[] constructorParam;
    private Class<?>[] parameterClasses;

    public WaveletFunction(int keyField, Object[] constructorParam){
        this.keyField = keyField;
        this.constructorParam = constructorParam;
        this.parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
    }

    public WaveletFunction(Object[] constructorParam){
        this.keyField = -1;
        this.constructorParam = constructorParam;
        this.parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
    }

    public WaveletSynopsis createAggregate() {
        try {
            Constructor<WaveletSynopsis> constructor = WaveletSynopsis.class.getConstructor(parameterClasses);
            return constructor.newInstance(constructorParam);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("MergeableSynopsis parameters didn't match any constructor");
        } catch (InstantiationException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Couldn't instantiate class");
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Access not permitted");
        } catch (InvocationTargetException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("InvocationTargetException");
        }
    }

    @Override
    public WaveletSynopsis lift(Tuple2<Integer, Input> inputTuple) {
        WaveletSynopsis partialAggregate = createAggregate();
        if(inputTuple.f1 instanceof Tuple && keyField != -1){
            Object field = ((Tuple) inputTuple.f1).getField(this.keyField);
            partialAggregate.update(field);
            return partialAggregate;
        }
        partialAggregate.update(inputTuple.f1);
        return partialAggregate;
    }

    @Override
    public WaveletSynopsis combine(WaveletSynopsis partialAggregate1, WaveletSynopsis partialAggregate2) {
        throw new IllegalArgumentException("Wavelet partial aggregates cannot be combined, because they are not mergeable.");
    }

    @Override
    public WaveletSynopsis liftAndCombine(WaveletSynopsis partialAggregate, Tuple2<Integer, Input> inputTuple) {
        if(inputTuple.f1 instanceof Tuple && keyField != -1){
            Object field = ((Tuple) inputTuple.f1).getField(this.keyField);
            partialAggregate.update(field);
            return partialAggregate;
        }
        partialAggregate.update(inputTuple.f1);
        return partialAggregate;
    }

    @Override
    public SliceWaveletsManager lower(WaveletSynopsis aggregate) {
        return new;
    }
}
