package Synopsis.Sampling;

import FlinkScottyConnector.BuildSynopsis;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 * Object to represent a sample element in the case the synopsis need the timestamp of each elements.
 * @see BuildSynopsis.ConvertToSample
 *
 * @param <T> the type of the elements
 *
 * @author Rudi Poepsel Lemaitre
 */
public class SampleElement<T> implements Serializable, Comparable<SampleElement> {
    private T value;
    private long timeStamp;

    /**
     * Construct a SampleElement with a given timestamp
     *
     * @param value value of the element
     * @param timeStamp of the element (It can be the Event-timestamp or Process-Timestamp)
     */
    public SampleElement(T value, long timeStamp) {
        this.value = value;
        this.timeStamp = timeStamp;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public long getTimeStamp() {
        return timeStamp;
    }


    @Override
    public int compareTo(SampleElement o) {
        int diff = (int) (this.timeStamp - o.timeStamp);
        if(diff == 0 && !o.getValue().equals(this.value)){
                return -1;
        }
        return diff;
    }

    @Override
    public boolean equals(Object  o){

        if (o == this) {
            return true;
        }

        /* Check if o is an instance of Complex or not
          "null instanceof [type]" also returns false */
        if (!(o instanceof SampleElement)) {
            return false;
        }

        // typecast o to Complex so that we can compare data members
        SampleElement c = (SampleElement) o;
        boolean equal=false;
        int diff = (int) (this.timeStamp - ((SampleElement) o).timeStamp);
        if(diff == 0 && ((SampleElement) o).getValue().equals(this.value)){
            equal=true;
        }
        return equal;
    }

    @Override
    public String toString() {
        return new String("(" + value.toString() + " | " + timeStamp + ")");
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.writeObject(value);
        out.writeLong(timeStamp);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        value = (T) in.readObject();
        timeStamp = in.readLong();
    }

    private void readObjectNoData() throws ObjectStreamException {
        throw new NotSerializableException("Serialization error in class " + this.getClass().getName());
    }
}
