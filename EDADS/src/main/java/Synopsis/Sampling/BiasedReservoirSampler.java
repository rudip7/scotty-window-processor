package Synopsis.Sampling;

import Synopsis.Synopsis;
import org.apache.flink.util.XORShiftRandom;

import java.io.Serializable;
import java.util.TreeMap;

/**
 * Implementation of the Biased Reservoir Synopsis.Sampling algorithm with a given sample size.
 * (@href http://charuaggarwal.net/sigreservoir.pdf)
 * The idea is to give more priority to the newest incoming elements adding them always to the sample in
 * contrast to the traditional Reservoir Sampler. The probability that this element is simply appended to
 * the sample or replace an element of the sample is given by actualSize/sampleSize. Meaning that once the sample
 * has reached the desired size the probability of an element replacing an already existing sample will be equal to 1.
 *
// * @param <T> the type of elements maintained by this sampler
 * @author Rudi Poepsel Lemaitre
 */
public class BiasedReservoirSampler<T> implements SamplerWithTimestamps<T>, Serializable {

    private SampleElement sample[];
    private int sampleSize;
    private XORShiftRandom rand;
    private int actualSize;
    private int merged = 1;
    private TreeMap<Long, Integer> latestPositions;

    /**
     * Construct a new empty Biased Reservoir Sampler with a bounded size.
     *
     * @param sampleSize
     */
    public BiasedReservoirSampler(Integer sampleSize) {
        this.sample = new SampleElement[sampleSize];
        this.sampleSize = sampleSize;
        this.rand = new XORShiftRandom();
        this.actualSize = 0;
        this.latestPositions = new TreeMap();
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
    public void update(SampleElement element) {
        if (latestPositions.isEmpty() || latestPositions.firstKey() < element.getTimeStamp()) {
            if (actualSize < sampleSize){
                sample[actualSize] = element;
                latestPositions.put(element.getTimeStamp(), actualSize);
                actualSize++;
            } else if (rand.nextDouble() < ((double) actualSize) / sampleSize) {
                Integer position = rand.nextInt(actualSize);
                latestPositions.remove(sample[position].getTimeStamp(), position);
                sample[position] = element;
                latestPositions.put(element.getTimeStamp(), position);
            }
        }
    }

    public SampleElement[] getSample() {
        return sample;
    }

    public int getSampleSize() {
        return sampleSize;
    }


    public TreeMap<Long, Integer> getLatestPositions() {
        return latestPositions;
    }

    public int getActualSize() {
        return actualSize;
    }

    /**
     * Function to Merge two Biased Reservoir samples. This function takes advantage of the ordering of the elements
     * given by the {@code FlinkScottyConnector.BuildSynopsis} retaining only the newest elements that entered the window.
     *
     * @param other Biased Reservoir sample to be merged with
     * @return merged Biased Reservoir Sample
     * @throws Exception
     */
    @Override
    public BiasedReservoirSampler merge(Synopsis other) {
        if (other instanceof BiasedReservoirSampler
                && ((BiasedReservoirSampler) other).getSampleSize() == this.sampleSize) {
            BiasedReservoirSampler<T> toMerge = (BiasedReservoirSampler<T>) other;
            BiasedReservoirSampler<T> mergeResult = new BiasedReservoirSampler(this.sampleSize);
            mergeResult.merged = this.merged + toMerge.merged;

            while (mergeResult.actualSize < this.sampleSize && !(this.getLatestPositions().isEmpty() && toMerge.getLatestPositions().isEmpty())) {
                if (!toMerge.getLatestPositions().isEmpty() && !this.getLatestPositions().isEmpty()){
                    if (toMerge.getLatestPositions().firstKey() < this.getLatestPositions().firstKey()){
                        Integer index = toMerge.getLatestPositions().pollFirstEntry().getValue();
                        mergeResult.update(toMerge.getSample()[index]);
                    } else{
                        Integer index = this.getLatestPositions().pollFirstEntry().getValue();
                        mergeResult.update(this.getSample()[index]);
                    }
                } else if(toMerge.getLatestPositions().isEmpty()){
                    Integer index = this.getLatestPositions().pollFirstEntry().getValue();
                    mergeResult.update(this.getSample()[index]);
                } else if(this.getLatestPositions().isEmpty()){
                    Integer index = toMerge.getLatestPositions().pollFirstEntry().getValue();
                    mergeResult.update(toMerge.getSample()[index]);
                }
            }
            return mergeResult;
        } else {
            throw new IllegalArgumentException("Reservoir Samplers to merge have to be the same size");
        }
    }


    @Override
    public String toString() {
        String s = new String("Biased Reservoir sample size: " + this.actualSize + "\n");
        for (int i = 0; i < actualSize; i++) {
            s += this.sample[i].toString() + ", ";
        }
        s = s.substring(0, s.length() - 2);
        s += "\n";
        return s;
    }

}
