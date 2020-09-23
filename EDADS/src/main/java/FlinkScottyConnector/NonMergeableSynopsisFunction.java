package FlinkScottyConnector;

import Synopsis.MergeableSynopsis;
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

public class NonMergeableSynopsisFunction<Input, S extends Synopsis, SM extends NonMergeableSynopsisManager> implements AggregateFunction<Input, NonMergeableSynopsisManager, NonMergeableSynopsisManager>, Serializable {
    private int keyField; // key value of the synopsis
    private Class<S> synopsisClass; //classes of synopsis
    private Class<SM> sliceManagerClass;
    private Object[] constructorParam; //parameters for constructors of each type of synopsis
    private Class<?>[] parameterClasses; // class type of constructor parameters

    /**
     * Construct a new NonMergeable Synopsis Function.
     *
     * @param keyField
     * @param partitionField
     * @param synopsisClass
     * @param sliceManagerClass
     * @param constructorParam
     * @throws IllegalArgumentException
     */
    public NonMergeableSynopsisFunction(int keyField, int partitionField, Class<S> synopsisClass, Class<SM> sliceManagerClass, Object[] constructorParam) {
        this.keyField = keyField;
        this.constructorParam = constructorParam;
        this.parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
        this.synopsisClass = synopsisClass;
        this.sliceManagerClass = sliceManagerClass;
        if (partitionField >= 0 && !StratifiedSynopsis.class.isAssignableFrom(synopsisClass)) {
            throw new IllegalArgumentException("Synopsis class needs to be a subclass of StratifiedSynopsis in order to build on personalized partitions.");
        }
    }


    /**
     * Construct a new NonMergeableSynopsisFunction.
     *
     * @param synopsisClass
     * @param sliceManagerClass
     * @param constructorParam
     */
    public NonMergeableSynopsisFunction(Class<S> synopsisClass, Class<SM> sliceManagerClass, Object[] constructorParam) {
        this.keyField = -1;
        this.constructorParam = constructorParam;
        this.parameterClasses = new Class[constructorParam.length];
        for (int i = 0; i < constructorParam.length; i++) {
            parameterClasses[i] = constructorParam[i].getClass();
        }
        this.synopsisClass = synopsisClass;
        this.sliceManagerClass = sliceManagerClass;
    }

    /**
     * Create a new instance of the nonmergable slice manager (Aggregate) and add synopsis with the specified constructor parameters to it.
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
        if (!(input instanceof Tuple2)) {
            throw new IllegalArgumentException("Input elements must be from type Tuple2 to build a synopsis.");
        }
        Tuple2 inputTuple = (Tuple2) input;
        NonMergeableSynopsisManager partialAggregate = createAggregate();
        if (inputTuple.f1 instanceof Tuple && keyField != -1) {
            Object field = ((Tuple) inputTuple.f1).getField(this.keyField);
            partialAggregate.update(field);
            return partialAggregate;
        }
        partialAggregate.update(inputTuple.f1);
        return partialAggregate;


    }

    /**
     * unify two NonMergeable Synopses collections
     *
     * @param input
     * @param partialAggregate
     */
    @Override
    public NonMergeableSynopsisManager combine(NonMergeableSynopsisManager input, NonMergeableSynopsisManager partialAggregate) {
        input.unify(partialAggregate);
        return input;
    }

    /**
     * add new element to a slice manager  (Aggregate) , the result is the same as invoking lift and then combine function.
     *
     * @param partialAggregate synopsis
     * @param input input element
     * @return updated synopsis
     */
    @Override
    public NonMergeableSynopsisManager liftAndCombine(NonMergeableSynopsisManager partialAggregate, Input input) {

        if (!(input instanceof Tuple2)) {
            throw new IllegalArgumentException("Input elements must be from type Tuple2 to build a synopsis.");
        }
        Tuple2 inputTuple = (Tuple2) input;
        if (inputTuple.f1 instanceof Tuple && keyField != -1) {
            Object field = ((Tuple) inputTuple.f1).getField(this.keyField);
            partialAggregate.update(field);
            return partialAggregate;
        }
        partialAggregate.update(inputTuple.f1);
        return partialAggregate;

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
        out.writeInt(keyField);
        out.writeObject(synopsisClass);
        out.writeObject(sliceManagerClass);
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
        this.keyField = in.readInt();
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
    }

    private void readObjectNoData() throws ObjectStreamException {
        System.out.println("readObjectNoData() called - should give an exception");
    }
}
