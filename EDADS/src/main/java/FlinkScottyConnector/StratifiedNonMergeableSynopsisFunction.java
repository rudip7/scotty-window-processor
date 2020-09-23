package FlinkScottyConnector;

import Synopsis.MergeableSynopsis;
import Synopsis.Sampling.TimestampedElement;
import Synopsis.StratifiedSynopsis;
import Synopsis.Synopsis;
import Synopsis.NonMergeableSynopsisManager;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.PriorityQueue;

public class StratifiedNonMergeableSynopsisFunction<Input, S extends Synopsis, SM extends NonMergeableSynopsisManager> implements AggregateFunction<Input, NonMergeableSynopsisManager, NonMergeableSynopsisManager>, Serializable {
    private Class<S> synopsisClass; // specific classes of synopsis
    private Class<SM> sliceManagerClass;
    private Object[] constructorParam; // parameters for constructors of each type of synopsis
    private Class<?>[] parameterClasses;// class type of constructor parameters
    private int miniBatchSize;// the size of batches to process
    private PriorityQueue<TimestampedElement<Tuple2>> dispatchList; //list of incoming element (used in case of batch processing)

    /**
     * Constructor.
     *
     * @param miniBatchSize
     * @param synopsisClass
     * @param sliceManagerClass
     * @param constructorParam
     */
    public StratifiedNonMergeableSynopsisFunction(int miniBatchSize, Class<S> synopsisClass, Class<SM> sliceManagerClass, Object[] constructorParam) {
        this.constructorParam = constructorParam;
        this.parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
        this.synopsisClass = synopsisClass;
        this.sliceManagerClass = sliceManagerClass;
        this.miniBatchSize = miniBatchSize;
        if (miniBatchSize > 0) {
            dispatchList = new PriorityQueue<>();
        }
    }

    /**
     * Constructor.
     *
     * @param synopsisClass
     * @param sliceManagerClass
     * @param constructorParam
     */
    public StratifiedNonMergeableSynopsisFunction(Class<S> synopsisClass, Class<SM> sliceManagerClass, Object[] constructorParam) {
        this.constructorParam = constructorParam;
        this.parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
        this.synopsisClass = synopsisClass;
        this.sliceManagerClass = sliceManagerClass;
    }

    /**
     * Create a new instance of the slice manager (Aggregate) and add synopsis with the specified constructor parameters to slice manager which is a collection of synopses.
     *
     * @return the slice manager
     * @throws IllegalArgumentException when there is no matching constructor, the specified class object cannot be
     * instantiated, access is not permitted or other exceptions thrown by invoked methods.
     */
    public NonMergeableSynopsisManager createAggregate() {
        try {
            Constructor<S> constructor = synopsisClass.getConstructor(parameterClasses);
            Constructor<SM> managerConstructor = sliceManagerClass.getConstructor();
            SM agg = managerConstructor.newInstance();
            agg.addSynopsis(constructor.newInstance(constructorParam));
            return agg;
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
     * Transforms a tuple to a partial aggregate,by updating aggregator with the tuple.
     *
     * @param input input element
     * @return updated synopsis aggregate
     */
    @Override
    public NonMergeableSynopsisManager lift(Input input) {
        if (miniBatchSize <= 0) {
            Tuple2 inputTuple;
            if (input instanceof TimestampedElement) {
                if (!(((TimestampedElement)input).getValue() instanceof Tuple2)) {
                    throw new IllegalArgumentException("Input elements must be from type Tuple2 to build a stratified non mergeable synopsis.");
                }
                inputTuple = (Tuple2) ((TimestampedElement) input).getValue();
            } else if (input instanceof Tuple2) {
                inputTuple = (Tuple2) input;
            } else {
                throw new IllegalArgumentException("Input elements must be from type Tuple2 to build a stratified non mergeable synopsis.");
            }
            NonMergeableSynopsisManager partialAggregate = createAggregate();
            partialAggregate.update(inputTuple.f1);
            partialAggregate.setPartitionValue(inputTuple.f0);
            return partialAggregate;
        } else {
            if (!(input instanceof TimestampedElement)) {
                throw new IllegalArgumentException("Input elements must be from type TimestampedElement to build a stratified non mergeable synopsis with order.");
            }
            NonMergeableSynopsisManager partialAggregate = createAggregate();
            TimestampedElement<Tuple2> inputTuple = (TimestampedElement<Tuple2>) input;
            partialAggregate.setPartitionValue(inputTuple.getValue().f0);
            dispatchList.add(inputTuple);
            if (dispatchList.size() == miniBatchSize) {
                while (!dispatchList.isEmpty()) {
                    Tuple2 tupleValue = dispatchList.poll().getValue();
                    partialAggregate.update(tupleValue.f1);
                }
            }
            return partialAggregate;
        }

    }

    /**
     * unify two Non-Mergeable Synopsis collections,
     *
     * @param input
     * @param partialAggregate
     * @return merged synopsis manager
     */
    @Override
    public NonMergeableSynopsisManager combine(NonMergeableSynopsisManager input, NonMergeableSynopsisManager partialAggregate) {
        Object partitionValue = partialAggregate.getPartitionValue();
        Object partitionValue2 = input.getPartitionValue();
        if (partitionValue != null
                && partitionValue2 == null) {
            input.setPartitionValue(partitionValue);
        } else if (partitionValue == null
                && partitionValue2 != null) {
            partialAggregate.setPartitionValue(partitionValue2);
        } else if (!partitionValue.equals(partitionValue2)) {
            throw new IllegalArgumentException("Some internal error occurred and the synopses to be merged have not the same partition value.");
        }
        input.unify(partialAggregate);
        return input;
    }

    /**
     * add new element to a synopsis manager (Aggregate) , the result is the same as invoking lift and then combine function.
     *
     * @param partialAggregate synopsis
     * @param input input element
     * @return updated synopsis
     */
    @Override
    public NonMergeableSynopsisManager liftAndCombine(NonMergeableSynopsisManager partialAggregate, Input input) {
        if (miniBatchSize <= 0) {
            Tuple2 inputTuple;
            if (input instanceof TimestampedElement) {
                if (!(((TimestampedElement)input).getValue() instanceof Tuple2)) {
                    throw new IllegalArgumentException("Input elements must be from type Tuple2 to build a stratified non mergeable synopsis.");
                }
                inputTuple = (Tuple2) ((TimestampedElement) input).getValue();
            } else if (input instanceof Tuple2) {
                inputTuple = (Tuple2) input;
            } else {
                throw new IllegalArgumentException("Input elements must be from type Tuple2 to build a stratified non mergeable synopsis.");
            }
            partialAggregate.update(inputTuple.f1);
            partialAggregate.setPartitionValue(inputTuple.f0);
            return partialAggregate;
        } else {
            if (!(input instanceof TimestampedElement)) {
                throw new IllegalArgumentException("Input elements must be from type TimestampedElement to build a stratified non mergeable synopsis with order.");
            }
            TimestampedElement<Tuple2> inputTuple = (TimestampedElement<Tuple2>) input;
            partialAggregate.setPartitionValue(inputTuple.getValue().f0);
            dispatchList.add(inputTuple);
            if (dispatchList.size() == miniBatchSize) {
                while (!dispatchList.isEmpty()) {
                    Tuple2 tupleValue = dispatchList.poll().getValue();
                    partialAggregate.update(tupleValue.f1);
                }
            }
            return partialAggregate;
        }
    }

    /**
     * returns the final synopsis manager
     * @param inputSynopsis
     * @return  the final InvertibleSynopsis
     */
    @Override
    public NonMergeableSynopsisManager lower(NonMergeableSynopsisManager inputSynopsis) {
        return inputSynopsis;
    }

    /**
     * Method needed for Serializability.
     * write object to an output Stream
     * @param out, output stream to write object to
     */
    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeObject(synopsisClass);
        out.writeObject(sliceManagerClass);
        out.writeInt(constructorParam.length);
        for (int i = 0; i < constructorParam.length; i++) {
            out.writeObject(constructorParam[i]);
        }
        for (int i = 0; i < constructorParam.length; i++) {
            out.writeObject(parameterClasses[i]);
        }
        out.writeInt(miniBatchSize);
        if (miniBatchSize > 0){
            out.writeObject(dispatchList);
        }
    }

    /**
     * Method needed for Serializability.
     * read object from an input Stream
     * @param in, input stream to read from
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        this.synopsisClass = (Class<S>) in.readObject();
        this.sliceManagerClass = (Class<SM>) in.readObject();
        int nParameters = in.readInt();
        this.constructorParam = new Object[nParameters];
        for (int i = 0; i < nParameters; i++) {
            constructorParam[i] = in.readObject();
        }
        this.parameterClasses = new Class<?>[nParameters];
        for (int i = 0; i < nParameters; i++) {
            parameterClasses[i] = (Class<?>) in.readObject();
        }
        this.miniBatchSize = in.readInt();
        if (miniBatchSize > 0){
            dispatchList = (PriorityQueue<TimestampedElement<Tuple2>>) in.readObject();
        }
    }

    private void readObjectNoData() throws ObjectStreamException {
        System.out.println("readObjectNoData() called - should give an exception");
    }
}
