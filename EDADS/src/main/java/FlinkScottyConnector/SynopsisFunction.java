package FlinkScottyConnector;

import Synopsis.MergeableSynopsis;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class SynopsisFunction<Input, T extends MergeableSynopsis> implements AggregateFunction<Tuple2<Integer,Input>, MergeableSynopsis, MergeableSynopsis>, Serializable {
    private int keyField;
    private Class<T> synopsisClass;
    private Object[] constructorParam;
    private Class<?>[] parameterClasses;

    public SynopsisFunction(int keyField, Class<T> synopsisClass, Object[] constructorParam){
        this.keyField = keyField;
        this.constructorParam = constructorParam;
        this.parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
        this.synopsisClass = synopsisClass;
    }

    public SynopsisFunction(Class<T> synopsisClass, Object[] constructorParam){
        this.keyField = -1;
        this.constructorParam = constructorParam;
        this.parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
        this.synopsisClass = synopsisClass;
    }

    public MergeableSynopsis createAggregate() {
        try {
            Constructor<T> constructor = synopsisClass.getConstructor(parameterClasses);
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
    public MergeableSynopsis lift(Tuple2<Integer,Input> inputTuple) {
        MergeableSynopsis partialAggregate = createAggregate();
        if(inputTuple.f1 instanceof Tuple && keyField != -1){
            Object field = ((Tuple) inputTuple.f1).getField(this.keyField);
            partialAggregate.update(field);
            return partialAggregate;
        }
        partialAggregate.update(inputTuple.f1);
        return partialAggregate;
    }

    @Override
    public MergeableSynopsis combine(MergeableSynopsis input, MergeableSynopsis partialAggregate) {
        try {
            return input.merge(partialAggregate);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public MergeableSynopsis liftAndCombine(MergeableSynopsis partialAggregate, Tuple2<Integer,Input> inputTuple) {
        if(inputTuple.f1 instanceof Tuple && keyField != -1){
            Object field = ((Tuple) inputTuple.f1).getField(this.keyField);
            partialAggregate.update(field);
            return partialAggregate;
        }
        partialAggregate.update(inputTuple.f1);
        return partialAggregate;
    }

    @Override
    public MergeableSynopsis lower(MergeableSynopsis inputInvertibleSynopsis) {
        return inputInvertibleSynopsis;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeInt(keyField);
        out.writeObject(synopsisClass);
        out.writeInt(constructorParam.length);
        for (int i = 0; i < constructorParam.length; i++) {
            out.writeObject(constructorParam[i]);
        }
        for (int i = 0; i < constructorParam.length; i++) {
            out.writeObject(parameterClasses[i]);
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        this.keyField = in.readInt();
        this.synopsisClass = (Class<T>) in.readObject();
        int nParameters = in.readInt();
        this.constructorParam = new Object[nParameters];
        for (int i = 0; i < nParameters; i++) {
            constructorParam[i] = in.readObject();
        }
        this.parameterClasses = new Class<?>[nParameters];
        for (int i = 0; i < nParameters; i++) {
            parameterClasses[i] = (Class<?>) in.readObject();
        }
    }

    private void readObjectNoData() throws ObjectStreamException {
        System.out.println("readObjectNoData() called - should give an exception");
    }
}
