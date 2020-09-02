package Synopsis.Sampling;

import Synopsis.MergeableSynopsis;
import Synopsis.StratifiedSynopsis;
import org.apache.flink.util.XORShiftRandom;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.TreeMap;

/**
 * Implementation of the Biased Reservoir MergeableSynopsis.Sampling algorithm with a given sample size.
 * (@href http://charuaggarwal.net/sigreservoir.pdf)
 * The idea is to give more priority to the newest incoming elements adding them always to the sample in
 * contrast to the traditional Reservoir Sampler. The probability that this element is simply appended to
 * the sample or replace an element of the sample is given by actualSize/sampleSize. Meaning that once the sample
 * has reached the desired size the probability of an element replacing an already existing sample will be equal to 1.
 * <p>
 * // * @param <T> the type of elements maintained by this sampler
 *
 * @author Rudi Poepsel Lemaitre
 */
public class BiasedReservoirSampler<T> extends StratifiedSynopsis implements SamplerWithTimestamps<T>, Serializable {

    private TimestampedElement sample[];// reservoir sampler
    private int sampleSize; // maximum possible sample size
    private XORShiftRandom rand; // random variable used to determine whether an element in sample should be replaced and which one
    private int actualSize;// the current size of the sample
    private int merged = 1;// number of synopsis that merged so far
    private LatestPositions latestPositions;// maintain the positions of timestamped elements in the sample

    /**
     * Construct a new empty Biased Reservoir Sampler with a bounded size.
     *
     * @param sampleSize
     */
    public BiasedReservoirSampler(Integer sampleSize) {
        this.sample = new TimestampedElement[sampleSize];
        this.sampleSize = sampleSize;
        this.rand = new XORShiftRandom();
        this.actualSize = 0;
        this.latestPositions = new LatestPositions();
    }

    /**
     * Add the incoming element to the sample. The probability that this element is simply appended to
     * the sample or replace an element of the sample is given by actualSize/sampleSize. Meaning that once the
     * sample has reached the desired size the probability of an element replacing an already existing sample
     * will be equal to 1.
     *
     * @param element
     */
    @Override
    public void update(TimestampedElement element) {
//        if (latestPositions.isEmpty() || latestPositions.oldestTimestamp() < element.getTimeStamp()) {
        if (actualSize < sampleSize) {
            sample[actualSize] = element;
            latestPositions.addElement(element.getTimeStamp(), actualSize);
            actualSize++;
        } else if (rand.nextDouble() < ((double) actualSize) / sampleSize) {
            Integer position = rand.nextInt(actualSize);
            latestPositions.removeElement(sample[position].getTimeStamp(), position);
            sample[position] = element;
            latestPositions.addElement(element.getTimeStamp(), position);
        }
//        }
    }

    /**
     * Returns the sample.
     *
     * @return the sample
     */
    public TimestampedElement[] getSample() {
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
     * Returns the latestPositions.
     *
     * @return the latestPositions
     */
    public LatestPositions getLatestPositions() {
        return latestPositions;
    }

    /**
     * Returns the actualSize.
     *
     * @return the actualSize
     */
    public int getActualSize() {
        return actualSize;
    }

    /**
     * Returns the merged.
     *
     * @return the merged
     */
    public int getMerged() {
        return merged;
    }

    /**
     * Function to Merge two Biased Reservoir samples. This function takes advantage of the ordering of the elements
     * given by the {@code FlinkScottyConnector.BuildSynopsis} retaining only the newest elements that entered the window.
     *
     * @param other Biased Reservoir sample to be merged with
     * @return merged Biased Reservoir Sample
     * @throws IllegalArgumentException
     */
    @Override
    public BiasedReservoirSampler merge(MergeableSynopsis other) {
        if (other instanceof BiasedReservoirSampler
                && ((BiasedReservoirSampler) other).getSampleSize() == this.sampleSize) {
            BiasedReservoirSampler<T> toMerge = (BiasedReservoirSampler<T>) other;
            BiasedReservoirSampler<T> mergeResult = new BiasedReservoirSampler(this.sampleSize);
            if (toMerge.getPartitionValue() != null) {
                mergeResult.setPartitionValue(toMerge.getPartitionValue());
            }
            mergeResult.merged = this.merged + toMerge.merged;

            int mergedSize = toMerge.getLatestPositions().nElements + this.getLatestPositions().nElements;
            if (mergedSize > this.sampleSize) {
                mergedSize = this.sampleSize;
            }
            while (mergeResult.getLatestPositions().nElements < mergedSize) {
                if (toMerge.getLatestPositions().isEmpty() && this.getLatestPositions().isEmpty()) {
                    System.out.println("wait");
                } else if (!toMerge.getLatestPositions().isEmpty() && !this.getLatestPositions().isEmpty()) {
                    if (toMerge.getLatestPositions().newestTimestamp() < this.getLatestPositions().newestTimestamp()) {
                        Integer index = toMerge.getLatestPositions().removeNewest();
                        mergeResult.update(toMerge.getSample()[index]);
                    } else {
                        Integer index = this.getLatestPositions().removeNewest();
                        mergeResult.update(this.getSample()[index]);
                    }
                } else if (toMerge.getLatestPositions().isEmpty()) {
                    Integer index = this.getLatestPositions().removeNewest();
                    mergeResult.update(this.getSample()[index]);
                } else if (this.getLatestPositions().isEmpty()) {
                    Integer index = toMerge.getLatestPositions().removeNewest();
                    mergeResult.update(toMerge.getSample()[index]);
                }

            }
            return mergeResult;
        } else {
            throw new IllegalArgumentException("Reservoir Samplers to merge have to be the same size");
        }
    }

    /**
     * convert the information contained in the sampler including the size and the elements to string .
     * could be used to print the sampler.
     *
     * @return a string of contained information
     */
    @Override
    public String toString() {
        String s = new String("Biased Reservoir sample size: " + this.actualSize + "\n");
        if (this.getPartitionValue() != null) {
            s += "partition = " + this.getPartitionValue().toString() + "\n";
        }
        for (int i = 0; i < actualSize; i++) {
            s += this.sample[i].toString() + ", ";
        }
        s = s.substring(0, s.length() - 2);
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
        for (int i = 0; i < sampleSize; i++) {
            out.writeObject(sample[i]);
        }
        out.writeInt(actualSize);
        out.writeObject(this.getPartitionValue());
        out.writeInt(merged);
        out.writeObject(latestPositions);

    }

    /**
     * Method needed for Serializability.
     * read object from an input Stream
     * @param in, input stream to read from
     */
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        sampleSize = in.readInt();
        for (int i = 0; i < sampleSize; i++) {
            sample[i] = (TimestampedElement) in.readObject();
        }
        actualSize = in.readInt();
        this.setPartitionValue(in.readObject());
        merged = in.readInt();
        latestPositions = (LatestPositions) in.readObject();
        this.rand = new XORShiftRandom();
    }

    private void readObjectNoData() throws ObjectStreamException {
        throw new NotSerializableException("Serialization error in class " + this.getClass().getName());
    }

    /**
     * the class maintain position of elements in the sample with their timestamps and contain functions to modify the sample
     */
    private class LatestPositions implements Serializable {
        TreeMap<Long, ArrayList<Integer>> positions; // stores timestamp and an array of elements with same timestamp
        int nElements;//number of elements

        /**
         * the constructor initialize variables
         */
        public LatestPositions() {
            positions = new TreeMap<>();
            nElements = 0;
        }

        /**
         * add position of new element to the structure
         *
         * @param timestamp time stamp off the added element
         * @param position
         */
        public void addElement(long timestamp, int position) {
            ArrayList<Integer> pos = positions.get(timestamp);
            if (pos == null) {
                pos = new ArrayList<>();
                pos.add(position);
                positions.put(timestamp, pos);
            } else {
                pos.add(position);
            }
            nElements++;
        }

        /**
         * remove the position of an element from the structure
         *
         * @param timeStamp time stamp off the removed element
         * @param position
         */
        public void removeElement(long timeStamp, Integer position) {
            nElements--;
            ArrayList<Integer> pos = positions.get(timeStamp);
            pos.remove((Object) position);
            if (pos.isEmpty()) {
                positions.remove(timeStamp);
            }
        }

        /**
         * returns whether the structure is empty or not
         *
         * @return false if it is not empty otherwise returns true
         */
        public boolean isEmpty() {
            if (nElements > 0) {
                return false;
            } else {
                return true;
            }
        }

        /**
         * remove the position of oldest added element
         *
         * @return the removed position otherwise -1
         */
        public int removeOldest() {
            if (nElements > 0) {
                nElements--;
                ArrayList<Integer> oldestList = positions.firstEntry().getValue();
                if (oldestList.size() == 1) {
                    positions.pollFirstEntry();
                    return oldestList.get(0);
                } else {
                    return oldestList.remove(0);
                }
            } else {
                return -1;
            }
        }

        /**
         * return the position of oldest added element
         *
         * @return the position otherwise -1
         */
        public int peekOldest() {
            if (nElements > 0) {
                return positions.firstEntry().getValue().get(0);
            } else {
                return -1;
            }
        }

        /**
         * remove the position of newest added element
         *
         * @return the removed position otherwise -1
         */
        public int removeNewest() {
            if (nElements > 0) {
                nElements--;
                ArrayList<Integer> newestList = positions.lastEntry().getValue();
                if (newestList.size() == 1) {
                    positions.pollLastEntry();
                    return newestList.get(newestList.size() - 1);
                } else {
                    return newestList.remove(newestList.size() - 1);
                }
            } else {
                return -1;
            }
        }

        /**
         * return the position of newest added element
         *
         * @return the position otherwise -1
         */
        public int peekNewest() {
            if (nElements > 0) {
                ArrayList<Integer> newestList = positions.lastEntry().getValue();
                return newestList.get(newestList.size() - 1);
            } else {
                return -1;
            }
        }

        /**
         * return the oldest timestamp
         *
         * @return the timestamp otherwise -1
         */
        public long oldestTimestamp() {
            if (nElements > 0) {
                return positions.firstKey();
            } else {
                return -1;
            }
        }

        /**
         * return the newest timestamp
         *
         * @return the timestamp otherwise -1
         */
        public long newestTimestamp() {
            if (nElements > 0) {
                return positions.lastKey();
            } else {
                return -1;
            }
        }

        /**
         * Method needed for Serializability.
         */
        private void writeObject(java.io.ObjectOutputStream out) throws IOException {
            out.writeInt(nElements);
            out.writeObject(positions);

        }

        private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
            nElements = in.readInt();
            positions = (TreeMap<Long, ArrayList<Integer>>) in.readObject();
        }

        private void readObjectNoData() throws ObjectStreamException {
            throw new NotSerializableException("Serialization error in class " + this.getClass().getName());
        }


    }
}
