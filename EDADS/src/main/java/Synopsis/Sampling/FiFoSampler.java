package Synopsis.Sampling;

import Synopsis.MergeableSynopsis;
import Synopsis.StratifiedSynopsis;
import org.apache.flink.streaming.api.TimeCharacteristic;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * this class maintain a sample base on the first in first out strategy
 * @param <T>
 */
public class FiFoSampler<T> extends StratifiedSynopsis implements SamplerWithTimestamps<T>, Serializable {
    //TODO: change treeset to priorityQueue due duplicated timestamps

    private TreeSet<TimestampedElement<T>> sample;
    private int sampleSize;//maximum possible sample size
    private boolean eventTime; //boolean display whether the type of timestampe is EventTime

    /**
     * the constructor- initialize variables and determine eventTime
     * @param sampleSize
     * @param timeCharacteristic
     */
    public FiFoSampler(Integer sampleSize, TimeCharacteristic timeCharacteristic) {
        this.sample = new TreeSet<>();
        this.sampleSize = sampleSize;
        if (timeCharacteristic == TimeCharacteristic.EventTime) {
            this.eventTime = true;
        } else {
            this.eventTime = false;
        }
    }

    /**
     * the constructor- if there is no timeCharacteristic, it is set to true by default
     * @param sampleSize
     */
    public FiFoSampler(Integer sampleSize) {
        this.sample = new TreeSet<>();
        this.sampleSize = sampleSize;
       this.eventTime=true;
    }

    /**
     * Update the sketch with a value T
     *
     * @param element
     */
    @Override
    public void update(TimestampedElement element) {
        if (sample.size() < sampleSize) {
            sample.add(element);
        } else if(sample.first().getTimeStamp() < element.getTimeStamp()){
            sample.pollFirst();
            sample.add(element);
        }

    }

    /**
     * Returns the sample.
     *
     * @return the sample
     */
    public TreeSet<TimestampedElement<T>> getSample() {
        return sample;
    }

    /**
     * Returns the sampleSize.
     *
     * @return the sampleSize
     */
    public int getSampleSize() {
        return sampleSize;
    }

    /**
     * Returns the eventTime.
     *
     * @return the eventTime
     */
    public boolean isEventTime() {
        return eventTime;
    }

    /**
     * Function to Merge two Fifo sampler. This function keeps the newest elements of two samplers in the result sample
     *
     * @param other, Fifo Reservoir sampler to be merged with
     * @return merged Fifo Sampler
     * @throws IllegalArgumentException
     */
    @Override
    public FiFoSampler merge(MergeableSynopsis other) {
        if (other instanceof FiFoSampler
                && ((FiFoSampler) other).getSampleSize() == this.sampleSize
                && ((FiFoSampler) other).isEventTime() == this.eventTime) {

            TreeSet<TimestampedElement<T>> otherSample = ((FiFoSampler) other).getSample();
            TreeSet<TimestampedElement<T>> mergeResult = new TreeSet<>();
            while (mergeResult.size() != sampleSize && !(otherSample.isEmpty() && this.sample.isEmpty())) {
                if (!otherSample.isEmpty() && !this.sample.isEmpty()){
                    if (otherSample.last().compareTo(this.sample.last()) > 0){
                        mergeResult.add(otherSample.pollLast());
                    } else {
                        mergeResult.add(this.sample.pollLast());
                    }
                } else if (otherSample.isEmpty()){
                    mergeResult.add(this.sample.pollLast());
                } else if (this.sample.isEmpty()){
                    mergeResult.add(otherSample.pollLast());
                }
            }
            this.sample = mergeResult;
        } else {
            throw new IllegalArgumentException("FiFoSamplers to merge have to be the same size");
        }
        return this;
    }

    /**
     * convert the information contained in the sampler including the size and the elements to string .
     * could be used to print the sampler.
     *
     * @return a string of contained information
     */
    @Override
    public String toString(){
        String s = new String("FiFo sample size: " + this.sampleSize+"\n");
        Iterator<TimestampedElement<T>> iterator = this.sample.iterator();
        while (iterator.hasNext()){
            s += iterator.next().toString()+", ";
        }
        s = s.substring(0,s.length()-2);
        s += "\n";
        return s;
    }

    /**
     * Method needed for Serializability.
     * write object to an output Stream
     * @param out, output stream to write object to
     */
    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeInt(sampleSize);
        out.writeObject(sample);
        out.writeBoolean(eventTime);
        out.writeObject(this.getPartitionValue());
    }

    /**
     * Method needed for Serializability.
     * read object from an input Stream
     * @param in, input stream to read from
     */
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        sampleSize = in.readInt();
        sample = (TreeSet<TimestampedElement<T>>) in.readObject();
        eventTime = in.readBoolean();
        this.setPartitionValue(in.readObject());
    }

    private void readObjectNoData() throws ObjectStreamException {
        throw new NotSerializableException("Serialization error in class " + this.getClass().getName());
    }

}
