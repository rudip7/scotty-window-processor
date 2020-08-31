package Synopsis.Wavelets;

import Synopsis.NonMergeableSynopsisManager;

import java.util.ArrayList;

public class DistributedSliceWaveletsManager<Input> extends NonMergeableSynopsisManager<Input,SliceWaveletsManager<Input>> {

    int parallelism = 0;

    /**
     * the constructor - set parallelism and elementsProcessed.
     *
     * @param unifiedSynopses an array of SliceWaveletsManager which is an array of WaveletSynopses
     */
    public DistributedSliceWaveletsManager(ArrayList<SliceWaveletsManager<Input>> unifiedSynopses) {
        this.unifiedSynopses = unifiedSynopses;
        this.parallelism = unifiedSynopses.size();
        for (int i = 0; i < unifiedSynopses.size(); i++) {
            elementsProcessed += unifiedSynopses.get(i).getElementsProcessed();
        }
    }

    /**
     * the constructor - when there is no parameter call NonMergeableSynopsisManager constructor.
     */
    public DistributedSliceWaveletsManager() {
        super();
    }

    /**
     * return the index of element in
     *
     * @param unifiedSynopses an array of SliceWaveletsManager which is an array of WaveletSynopses
     */

    /**
     * return the index of element with  streamIndex in its partition
     *
     * @param streamIndex
     */
    @Override
    public int getSynopsisIndex(int streamIndex) {
        return streamIndex % parallelism;
    }

    /**
     * Update the structure with a new incoming element.
     *
     * @param element new incoming element
     */
    @Override
    public void update(Object element) {
        elementsProcessed++;
        unifiedSynopses.get(getSynopsisIndex(elementsProcessed)).update(element);
    }

    @Override
    /**
     * add new SliceWaveletManager to the unified partitions
     *
     * @param synopsis
     */
    public void addSynopsis(SliceWaveletsManager<Input> synopsis) {
        parallelism++;
        elementsProcessed += synopsis.getElementsProcessed();
        super.addSynopsis(synopsis);
    }

    /**
     * show in which partition the index is contained
     *
     * @param index
     */
    public int getLocalIndex(int index) {
        return index / parallelism;
    }

    /**
     * perform a simple point query based on the given index
     *
     * @param index
     * @return value of the stream element at given index
     */
    public double pointQuery(int index) {
        return unifiedSynopses.get(getSynopsisIndex(index)).pointQuery(getLocalIndex(index));
    }

    /**
     * performs a range sum query.
     *
     * @param leftIndex
     * @param rightIndex
     * @return approximated sum of values between leftIndex and rightIndex
     */
    public double rangeSumQuery(int leftIndex, int rightIndex) {
        double rangeSum = 0;

        int leftLocalIndex = getLocalIndex(leftIndex);
        int rightLocalIndex = getLocalIndex(rightIndex);

        for (int i = 0; i < parallelism; i++) {

            int partitionLeftIndex = leftLocalIndex;
            if (getGlobalIndex(leftLocalIndex, i) < leftIndex) {
                partitionLeftIndex += 1;
            }

            int partitionRightIndex = rightLocalIndex;
            if (getGlobalIndex(rightLocalIndex, i) > rightIndex) {
                partitionRightIndex -= 1;
            }

            rangeSum += unifiedSynopses.get(i).rangeSumQuery(partitionLeftIndex, partitionRightIndex);
        }

        return rangeSum;
    }

    /**
     * return the index of element in the whole stream
     *
     * @param localIndex index of partition
     * @param partition index of element in partition
     */
    private int getGlobalIndex(int localIndex, int partition) {
        return partition + (localIndex * parallelism);
    }
}
