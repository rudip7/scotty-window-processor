package Synopsis.Wavelets;

import Synopsis.NonMergeableSynopsisManager;

import java.util.ArrayList;

public class DistributedWaveletsManager<Input> extends NonMergeableSynopsisManager<Input,WaveletSynopsis<Input>> {

    int parallelism;

    /**
     * the constructor - set parallelism and the unified synopses.
     *
     * @param unifiedSynopses an array of SliceWaveletsManager which is an array of WaveletSynopses
     */
    public DistributedWaveletsManager(int parallelism, ArrayList<WaveletSynopsis<Input>> unifiedSynopses) {
        this.parallelism = parallelism;
        this.unifiedSynopses = unifiedSynopses;
    }

    public DistributedWaveletsManager(){
        super();
    }

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
        unifiedSynopses.get(getSynopsisIndex(elementsProcessed)).update((Input) element);
    }

    /**
     * add new WaveletSynopsis to the structure.
     *
     * @param element new incoming element
     */
    @Override
    public void addSynopsis(WaveletSynopsis<Input> synopsis) {
        parallelism++;
        elementsProcessed += synopsis.getStreamElementCounter();
        super.addSynopsis(synopsis);
    }

    /**
     * show in which partition the index is contained
     *
     * @param index
     */
    public int getLocalIndex(int index){
        return index / parallelism;
    }

    /**
     * perform a simple point query based on the given index
     *
     * @param index
     * @return value of the stream element at given index
     */
    public double pointQuery(int index){
        WaveletSynopsis<Input> wavelet = unifiedSynopses.get(getSynopsisIndex(index));
        int localIndex = getLocalIndex(index);
        if (localIndex > wavelet.getStreamElementCounter()){
            System.out.println("ups");
            localIndex = getLocalIndex(index);
            return -1;
        }
        return wavelet.pointQuery(localIndex);
//        return unifiedSynopses.get(getSynopsisIndex(index)).pointQuery(getLocalIndex(index));
    }

    /**
     * performs a range sum query.
     *
     * @param leftIndex
     * @param rightIndex
     * @return approximated sum of values between leftIndex and rightIndex
     */

    public double rangeSumQuery(int leftIndex, int rightIndex){
        double rangeSum = 0;

        int leftLocalIndex = getLocalIndex(leftIndex);
        int rightLocalIndex = getLocalIndex(rightIndex);

        for (int i = 0; i < parallelism; i++) {

            int partitionLeftIndex = leftLocalIndex;
            if (getGlobalIndex(leftLocalIndex, i) < leftIndex){
                partitionLeftIndex += 1;
            }

            int partitionRightIndex = rightLocalIndex;
            if (getGlobalIndex(rightLocalIndex, i) > rightIndex){
                partitionRightIndex -=1;
            }

            rangeSum += unifiedSynopses.get(i).rangeSumQuery(partitionLeftIndex, partitionRightIndex);
        }

        return rangeSum;
    }

    private int getGlobalIndex(int localIndex, int partition){
        return partition + (localIndex * parallelism);
    }
}
