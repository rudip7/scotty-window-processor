package Synopsis.Sampling;

import Synopsis.CommutativeSynopsis;
import Synopsis.MergeableSynopsis;
import Synopsis.StratifiedSynopsis;
import org.apache.flink.util.XORShiftRandom;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;

/**
 * Implementation of the classic Reservoir MergeableSynopsis.Sampling algorithm with a given sample size.
 * Firstly the sample will be filled with every incoming element, once the sample has reached the sampleSize
 * bound the upcoming elements will be added to the sample with a probability of sampleSize/processedElements.
 * If an element should be added it will replace a random element of the sample.
 *
 * @param <T> the type of elements maintained by this sampler
 *
 * @author Rudi Poepsel Lemaitre
 */
public class ReservoirSampler<T> extends StratifiedSynopsis implements CommutativeSynopsis<T>, Serializable {
    private T sample[];
    private int sampleSize;
    private XORShiftRandom rand;
    private int processedElements;

    /**
     * Construct a new empty Reservoir Sampler with a bounded size.
     *
     * @param sampleSize maximal sample size
     */
    public ReservoirSampler(Integer sampleSize) {
        this.sample = (T[]) new Object[sampleSize];
        this.sampleSize = sampleSize;
        this.rand = new XORShiftRandom();
        this.processedElements = 0;
    }

    /**
     * Process a new incoming element. Firstly the sample will be fulled with every incoming element,
     * once the sample has reached the sampleSize bound the upcoming elements will be added to the sample
     * with a probability of sampleSize/processedElements. If an element should be added it will replace
     * a random element of the sample.
     *
     * @param element element to be added to the sample
     */
    @Override
    public void update(T element) {
        if (processedElements < sampleSize) {
            sample[processedElements] = element;
            processedElements++;
        } else {
            processedElements++;
            if (rand.nextDouble() < ((double) sampleSize)/processedElements){
                sample[rand.nextInt(sampleSize)] = element;
            }
        }
    }

    /**
     * Returns the sample.
     *
     * @return the sample
     */
    public T[] getSample() {
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
     * Returns the processedElements.
     *
     * @return the processedElements
     */
    public int getProcessedElements() {
        return processedElements;
    }

    /**
     * Function to Merge two Reservoir Samplers, since the the probability of an element staying in the sample
     * decreases with the time the merge function will weight the probability of a random item coming from each
     * sampler by the number of processed elements of each sampler.
     *
     * @param other Reservoir Sampler to be merged with this Reservoir Sampler
     * @return the merged Reservoir Sampler
     * @throws IllegalArgumentException
     */
    @Override
    public ReservoirSampler<T> merge(MergeableSynopsis other) {
        if (other instanceof ReservoirSampler
                && ((ReservoirSampler) other).getSampleSize() == this.sampleSize) {
            ReservoirSampler<T> o = (ReservoirSampler<T>) other;
            T[] otherSample = o.getSample();
            T[] mergeResult = (T[]) new Object[sampleSize];
            int actual = 0;
            ArrayList<Integer> indicesList1;
            if (this.processedElements > this.sampleSize){
                indicesList1 = generateIndicesArray(this.sampleSize);
            } else{
                indicesList1 = generateIndicesArray(this.processedElements);
            }

            ArrayList<Integer> indicesList2;
            if (o.getProcessedElements() > o.getSampleSize()){
                indicesList2 = generateIndicesArray(o.getSampleSize());
            } else{
                indicesList2 = generateIndicesArray(o.getProcessedElements());
            }

            double prob = ((double)this.processedElements)/(this.processedElements+o.getProcessedElements());
            while (actual != sampleSize && !(indicesList1.isEmpty() && indicesList2.isEmpty())) {
                if (rand.nextDouble() < prob){
                    if (!indicesList1.isEmpty()){
                        mergeResult[actual] = this.sample[getIndexWithoutReplacement(indicesList1)];
                        actual++;
                    } else {
                        mergeResult[actual] = otherSample[getIndexWithoutReplacement(indicesList2)];
                        actual++;
                    }
                } else{
                    if (!indicesList2.isEmpty()){
                        mergeResult[actual] = otherSample[getIndexWithoutReplacement(indicesList2)];
                        actual++;
                    } else {
                        mergeResult[actual] = this.sample[getIndexWithoutReplacement(indicesList1)];
                        actual++;
                    }
                }
            }
            this.sample = mergeResult;
            this.processedElements += o.getProcessedElements();
        } else {
            throw new IllegalArgumentException("Reservoir Samplers to merge have to be the same size");
        }
        return this;
    }

    /**
     * private function to generate an array list containing all indices from 0 to size-1.
     *
     * @param size
     * @return
     */
    private ArrayList<Integer> generateIndicesArray(int size){
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            list.add(i);
        }
        return list;
    }

    /**
     * private function to pick a random index without replacement from the given index list.
     *
     * @param list containing the remaining possible indices
     * @return a random index
     */
    private int getIndexWithoutReplacement(ArrayList<Integer> list){
        return list.remove(rand.nextInt(list.size()));
    }

    /**
     * Merge two Reservoir Samplers without taking in account how many elements were processed and giving an
     * equal distribution coming from each sample.
     *
     * @param other Reservoir Sampler to be merged with
     * @return merged Reservoir Sampler
     * @throws Exception
     */
    public ReservoirSampler<T> nonWeightedMerge(ReservoirSampler<T> other) throws Exception {
        if (other.getSampleSize() == this.sampleSize) {
            T[] otherSample = other.getSample();
            T[] mergeResult = (T[]) new Object[sampleSize];
            int actual = 0;
            ArrayList<Integer> indicesList1;
            if (this.processedElements > this.sampleSize){
                indicesList1 = generateIndicesArray(this.sampleSize);
            } else{
                indicesList1 = generateIndicesArray(this.processedElements);
            }

            ArrayList<Integer> indicesList2;
            if (other.getProcessedElements() > other.getSampleSize()){
                indicesList2 = generateIndicesArray(other.getSampleSize());
            } else{
                indicesList2 = generateIndicesArray(other.getProcessedElements());
            }

            while (actual != sampleSize && !(indicesList1.isEmpty() && indicesList2.isEmpty())) {
                if (!indicesList1.isEmpty()){
                    mergeResult[actual] = this.sample[getIndexWithoutReplacement(indicesList1)];
                    actual++;
                } if (actual != sampleSize && !indicesList2.isEmpty()){
                    mergeResult[actual] = otherSample[getIndexWithoutReplacement(indicesList2)];
                    actual++;
                }
            }
            this.sample = mergeResult;
            this.processedElements += other.getProcessedElements();
        } else {
            throw new Exception("Reservoir Samplers to merge have to be the same size");
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
        String s = new String("Reservoir sample size: " + this.sampleSize+"\n");
        if (this.getPartitionValue() != null) {
            s += "partition = " + this.getPartitionValue().toString()+"\n";
        }
        for (int i = 0; i < processedElements; i++) {
            if (i == sampleSize){
                break;
            }
            s += this.sample[i].toString()+", ";
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
        for (int i = 0; i < sampleSize; i++) {
            out.writeObject(sample[i]);
        }
        out.writeInt(processedElements);
        out.writeObject(this.getPartitionValue());
    }

    /**
     * Method needed for Serializability.
     * read object from an input Stream
     * @param in, input stream to read from
     */
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        sampleSize = in.readInt();
        for (int i = 0; i < sampleSize; i++) {
            sample[i] = (T) in.readObject();
        }
        processedElements = in.readInt();
        this.setPartitionValue(in.readObject());
        this.rand = new XORShiftRandom();
    }

    private void readObjectNoData() throws ObjectStreamException {
        throw new NotSerializableException("Serialization error in class " + this.getClass().getName());
    }

}
