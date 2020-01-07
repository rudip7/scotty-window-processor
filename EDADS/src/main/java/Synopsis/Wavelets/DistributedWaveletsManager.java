package Synopsis.Wavelets;

import Synopsis.NonMergeableSynopsis;

import java.util.ArrayList;

public class DistributedWaveletsManager<Input> extends NonMergeableSynopsis<Input, WaveletSynopsis<Input>> {

    private ArrayList<SliceWaveletsManager<Input>> combinedSynopses;
    int parallelism;
    int elementCounter = 0;

    public DistributedWaveletsManager(int parallelism, int size, ArrayList<SliceWaveletsManager<Input>> combinedSynopses) {
        this.combinedSynopses = combinedSynopses;
        this.parallelism = parallelism;
    }

    @Override
    public int getSynopsisIndex(int streamIndex) {

        return streamIndex % parallelism;
    }

    @Override
    public void update(Input element) {
        elementCounter++;
        combinedSynopses.get(getSynopsisIndex(elementCounter)).update(element);
    }

    public int getLocalIndex(int index){
        return index / parallelism;
    }

    public double pointQuery(int index){
        return combinedSynopses.get(getSynopsisIndex(index)).pointQuery(getLocalIndex(index));
    }

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

            rangeSum += combinedSynopses.get(i).rangeSumQuery(partitionLeftIndex, partitionRightIndex);
        }

        return rangeSum;
    }

    private int getGlobalIndex(int localIndex, int partition){
        return partition + (localIndex * parallelism);
    }
}
