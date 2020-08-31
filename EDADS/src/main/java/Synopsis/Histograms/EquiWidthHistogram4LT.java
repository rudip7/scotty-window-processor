package Synopsis.Histograms;

import java.util.Arrays;

/**
 * Creates an Equi-width histogram using 4LT buckets from an ordinary equi-width histogram
 * @author joschavonhein
 */
public class EquiWidthHistogram4LT {
    double lowerBound; //lower bound of histogram range
    double upperBound; //upper bound of histogram range
    int numBuckets; //number of histogram buckets
    double bucketLength; //length of buckets in histogram
    RealValuedBucket4LT[] buckets; // array stores buckets of histogram

    /**
     * constructor - creates a new Equi-width histogram through building 4LT buckets based on transformed information of old ordinary equi-width histogram
     * @param old, the ordinary equiwidth histogram
     */
    public EquiWidthHistogram4LT(EquiWidthHistogram old) throws Exception {
        lowerBound = old.getLowerBound();
        upperBound = old.getUpperBound();
        int oldNumBuckets = old.getNumBuckets();
        double oldBucketLength = (upperBound-lowerBound) / oldNumBuckets;
        bucketLength = oldBucketLength * 8;
        numBuckets = (int) Math.ceil(oldNumBuckets / 8d);
        int[] oldFrequencies = old.getFrequency();
        int extraEmptyBuckets = oldNumBuckets % 8;
        upperBound += extraEmptyBuckets * oldBucketLength;
        buckets = new RealValuedBucket4LT[numBuckets];
        int[] moduloEightFrequencies = Arrays.copyOf(oldFrequencies, oldNumBuckets+extraEmptyBuckets);

        for (int i = 0; i < numBuckets; i++) {
            buckets[i] = new RealValuedBucket4LT(lowerBound + bucketLength*i, lowerBound + bucketLength * (i+1));
            int[] bucketFrequencies = Arrays.copyOfRange(moduloEightFrequencies, i*8, (i*8)+8);
            buckets[i].build(bucketFrequencies);
        }
    }

    /**
     *performs a range query
     *
     * @param lowerBound
     * @param upperBound
     * @return the approximate frequency of queried range
     */
    public int rangeQuery(double lowerBound, double upperBound){
        int result = 0;
        int leftIndex = Math.max((int)((lowerBound-this.lowerBound)/bucketLength),0);
        int rightIndex = Math.min(numBuckets-1,(int) Math.ceil((upperBound-this.lowerBound)/bucketLength));
        for (int i = leftIndex; i < rightIndex; i++) {
            result += buckets[i].getFrequency(lowerBound, upperBound);
        }
        return  result;
    }

    /**
     * Returns the lowerbound.
     *
     * @return the lowerbound
     */
    public double getLowerBound() {
        return lowerBound;
    }

    /**
     * Returns the upperBound.
     *
     * @return the upperBound
     */
    public double getUpperBound() {
        return upperBound;
    }

    /**
     * Returns the numBuckets.
     *
     * @return the numBuckets
     */
    public int getNumBuckets() {
        return numBuckets;
    }

    /**
     * Returns the bucketLength.
     *
     * @return the bucketLength
     */
    public double getBucketLength() {
        return bucketLength;
    }

    /**
     * Returns the buckets.
     *
     * @return the buckets
     */
    public RealValuedBucket4LT[] getBuckets() {
        return buckets;
    }

    /**
     * merge 2 EquiWidthHistogram4LT
     *
     * @param other the histogram to merge with
     * @throws IllegalArgumentException
     */
    public EquiWidthHistogram4LT merge(EquiWidthHistogram4LT other){
        if (other.getLowerBound() != this.lowerBound || other.getUpperBound() != this.upperBound
            || other.getNumBuckets() != this.getNumBuckets()){
            throw new IllegalArgumentException("MergeableSynopsis.Histograms to need to have the same boundaries and the same number of buckets!");
        }
        // TODO: implement this
        return null;
    }

    /**
     * convert the information contained in EquiWidthHistogram4LT to string.
     * could be used to print the histogram
     *
     * @return a string of contained information
     */
    public String toString(){
        String s ="";
        for (int i = 0; i < numBuckets; i++) {
            s += buckets[i].toString() + "\n\n";
        }
        return s;
    }
}
