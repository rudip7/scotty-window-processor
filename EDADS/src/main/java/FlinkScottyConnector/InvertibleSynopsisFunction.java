package FlinkScottyConnector;

import Synopsis.Sketches.CountMinSketch;
import Synopsis.InvertibleSynopsis;
import Synopsis.StratifiedSynopsis;
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

public class InvertibleSynopsisFunction<Input extends Tuple2, T extends InvertibleSynopsis> implements InvertibleAggregateFunction<Input, InvertibleSynopsis, InvertibleSynopsis>, CommutativeAggregateFunction<Input, InvertibleSynopsis, InvertibleSynopsis>, Serializable {
    private Class<T> synopsisClass; // specific classes of synopsis
    private Object[] constructorParam; // parameters for constructors of each type of synopsis
    private Class<?>[] parameterClasses; // class type of constructor parameters
    private boolean stratified = false;


    /**
     * the constructor- creates a InvertibleSynopsisFunction when stratified parameter is specified too.
     *
     * @param stratified
     * @param synopsisClass
     * @param constructorParam
     * @throws IllegalArgumentException when the synopsis class is not subclass of StratifiedSynopsis but stratified is True
     */
    public InvertibleSynopsisFunction(boolean stratified, Class<T> synopsisClass, Object... constructorParam) {
        this.constructorParam = constructorParam;
        this.parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
        this.synopsisClass = synopsisClass;
        if (stratified && !StratifiedSynopsis.class.isAssignableFrom(synopsisClass)) {
            throw new IllegalArgumentException("Synopsis class needs to be a subclass of StratifiedSynopsis in order to build on personalized partitions.");
        }
        this.stratified = stratified;
    }

    /**
     * the constructor- creates a InvertibleSynopsisFunction and initialize the attributes
     *
     * @param synopsisClass
     * @param constructorParam
     */
    public InvertibleSynopsisFunction(Class<T> synopsisClass, Object... constructorParam) {
        this.constructorParam = constructorParam;
        this.parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
        this.synopsisClass = synopsisClass;
    }

    /**
     * Create a new instance of the synopsis (Aggregate) with the specified constructor parameters
     *
     * @return a new aggregation created by calling the constructor
     * @throws IllegalArgumentException when there is no matching constructor, the specified class object cannot be
     * instantiated, access is not permitted or other exceptions thrown by invoked methods.
     */
    public InvertibleSynopsis createAggregate() {
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

    /**
     * remove one synopsis from another synopsis
     *
     * @param partialAggregate //main synopsis
     * @param toRemove // synopsis that should be removed
     * @return inverted synopsis
     */
    @Override
    public InvertibleSynopsis invert(InvertibleSynopsis partialAggregate, InvertibleSynopsis toRemove) {
        try {
            return partialAggregate.invert(toRemove);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * remove one element from the synopsis (Aggregate), the result is the same as invoking lift and then invert function.
     *
     * @param partialAggregate //synopsis
     * @param inputTuple // element that should be removed
     * @return result synopsis after removing
     */
    @Override
    public InvertibleSynopsis liftAndInvert(InvertibleSynopsis partialAggregate, Input inputTuple) {
        if (stratified) {
            ((StratifiedSynopsis) partialAggregate).setPartitionValue(inputTuple.f0);
        }
        partialAggregate.decrement(inputTuple.f1);
        return partialAggregate;
    }

    /**
     *Transforms a tuple to a partial aggregate,by updating aggregator with the tuple
     *
     * @param inputTuple input element
     * @return updated synopsis
     */
    @Override
    public InvertibleSynopsis lift(Input inputTuple) {
        InvertibleSynopsis partialAggregate = createAggregate();
        if (stratified) {
            ((StratifiedSynopsis) partialAggregate).setPartitionValue(inputTuple.f0);
        }
        partialAggregate.update(inputTuple.f1);
        return partialAggregate;
    }

    /**
     * merge two Invertible Synopsis
     * This method can be used to add a single tuple partial aggregate to a larger aggregate
     * or to merge two big aggregates (or slices)
     * @param input
     * @param partialAggregate
     * @return merged synopsis
     */
    @Override
    public InvertibleSynopsis combine(InvertibleSynopsis input, InvertibleSynopsis partialAggregate) {
        try {
            return input.merge(partialAggregate);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * add new element to a synopsis (Aggregate), the result is the same as invoking lift and then combine function.
     *
     * @param partialAggregate synopsis
     * @param inputTuple input element
     * @return updated synopsis
     */
    @Override
    public InvertibleSynopsis liftAndCombine(InvertibleSynopsis partialAggregate, Input inputTuple) {
        if (stratified) {
            ((StratifiedSynopsis) partialAggregate).setPartitionValue(inputTuple.f0);
        }
        partialAggregate.update(inputTuple.f1);
        return partialAggregate;
    }

    /**
     * returns the final synopsis
     * @param inputInvertibleSynopsis
     * @return  the final InvertibleSynopsis
     */
    @Override
    public InvertibleSynopsis lower(InvertibleSynopsis inputInvertibleSynopsis) {
        return inputInvertibleSynopsis;
    }

    /**
     * Method needed for Serializability.
     * write object to an output Stream
     * @param out, output stream to write object to
     */
    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeBoolean(stratified);
        out.writeObject(synopsisClass);
        out.writeInt(constructorParam.length);
        for (int i = 0; i < constructorParam.length; i++) {
            out.writeObject(constructorParam[i]);
        }
        for (int i = 0; i < constructorParam.length; i++) {
            out.writeObject(parameterClasses[i]);
        }
    }

    /**
     * Method needed for Serializability.
     * read object from an input Stream
     * @param in, input stream to read from
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        this.stratified = in.readBoolean();
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
