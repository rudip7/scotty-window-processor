package FlinkScottyConnector;

import Synopsis.Sketches.CountMinSketch;
import Synopsis.InvertibleSynopsis;
import de.tub.dima.scotty.core.windowFunction.CommutativeAggregateFunction;
import de.tub.dima.scotty.core.windowFunction.InvertibleAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class InvertibleSynopsisFunction<Input,T extends InvertibleSynopsis> implements InvertibleAggregateFunction<Tuple2<Integer,Input>, InvertibleSynopsis, InvertibleSynopsis>, CommutativeAggregateFunction<Tuple2<Integer,Input>, InvertibleSynopsis, InvertibleSynopsis>, Serializable {
    private int keyField;
    private Class<T> synopsisClass;
    private Object[] constructorParam;
    private Class<?>[] parameterClasses;


    public InvertibleSynopsisFunction(int keyField, Class<T> synopsisClass, Object... constructorParam){
        this.keyField = keyField;
        this.constructorParam = constructorParam;
        this.parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
        this.synopsisClass = synopsisClass;
    }

    public InvertibleSynopsisFunction(Class<T> synopsisClass, Object... constructorParam){
        this.keyField = -1;
        this.constructorParam = constructorParam;
        this.parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
        this.synopsisClass = synopsisClass;
    }

    public InvertibleSynopsis createAggregate() {
        try {
            Constructor<T> constructor = synopsisClass.getConstructor(parameterClasses);
            return constructor.newInstance(constructorParam);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Synopsis parameters didn't match any constructor");
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
    public InvertibleSynopsis invert(InvertibleSynopsis partialAggregate, InvertibleSynopsis toRemove) {
        try {
            return partialAggregate.invert(toRemove);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public InvertibleSynopsis liftAndInvert(InvertibleSynopsis partialAggregate, Tuple2<Integer,Input> toRemove) {
        if(toRemove.f1 instanceof Tuple && keyField != -1){
            Input field = ((Tuple) toRemove.f1).getField(this.keyField);
            partialAggregate.decrement(field);
            return partialAggregate;
        }
        partialAggregate.decrement(toRemove.f1);
        return partialAggregate;
    }

    @Override
    public InvertibleSynopsis lift(Tuple2<Integer,Input> inputTuple) {
        InvertibleSynopsis partialAggregate = createAggregate();
        if(inputTuple.f1 instanceof Tuple && keyField != -1){
            Object field = ((Tuple) inputTuple.f1).getField(this.keyField);
            partialAggregate.update(field);
            return partialAggregate;
        }
        partialAggregate.update(inputTuple.f1);
        return partialAggregate;
    }

    @Override
    public InvertibleSynopsis combine(InvertibleSynopsis input, InvertibleSynopsis partialAggregate) {
        try {
            return input.merge(partialAggregate);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public InvertibleSynopsis liftAndCombine(InvertibleSynopsis partialAggregate, Tuple2<Integer,Input> inputTuple) {
        if(inputTuple.f1 instanceof Tuple && keyField != -1){
            Object field = ((Tuple) inputTuple.f1).getField(this.keyField);
            partialAggregate.update(field);
            return partialAggregate;
        }
        partialAggregate.update(inputTuple.f1);
        return partialAggregate;
    }

    @Override
    public InvertibleSynopsis lower(InvertibleSynopsis inputInvertibleSynopsis) {
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
