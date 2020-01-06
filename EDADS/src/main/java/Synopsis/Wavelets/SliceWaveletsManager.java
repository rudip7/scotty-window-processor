package Synopsis.Wavelets;

import Synopsis.NonMergeableSynopsis;

import java.util.ArrayList;
import java.util.Arrays;


public class SliceWaveletsManager<Input> extends NonMergeableSynopsis<Input, DistributedWaveletsManager<Input>> {

    ArrayList<WaveletSynopsis<Input>> slices;
    int slicesPerWindow;
    int[] sliceStartIndices;

    public SliceWaveletsManager(ArrayList<WaveletSynopsis<Input>> slices, int slicesPerWindow) {
        this.slices = slices;
        this.slicesPerWindow = slicesPerWindow;
        sliceStartIndices = new int[slicesPerWindow];

        int previousSliceElements = 0;
        for (int i = 0; i < slicesPerWindow; i++) {
            sliceStartIndices[i] = previousSliceElements;

            previousSliceElements += slices.get(i).getStreamElementCounter();
        }
    }

    @Override
    public int getSynopsisIndex(int streamIndex) {
        return 0;
    }

    public double pointQuery(int index){
        int managerIndex = Arrays.binarySearch(sliceStartIndices, index);
        int previousSliceElements = sliceStartIndices[managerIndex];
        return slices.get(managerIndex).pointQuery(index - previousSliceElements);
    }


    public double rangeSumQuery(int leftIndex, int rightIndex){
        int leftManagerIndex = Arrays.binarySearch(sliceStartIndices, leftIndex);
        int rightManagerIndex = Arrays.binarySearch(sliceStartIndices, rightIndex);

        double rangeSum = 0;

        for (int i = leftManagerIndex; i <= rightManagerIndex; i++) {
            int previousSliceElements = sliceStartIndices[i];
            int localLeftIndex = i == leftManagerIndex ? leftIndex-previousSliceElements : 0;
            int localRightIndex = i == rightManagerIndex ? rightIndex-previousSliceElements : sliceStartIndices[i+1] - previousSliceElements;
            rangeSum += slices.get(i).rangeSumQuery(localLeftIndex, localRightIndex);
        }
        return rangeSum;
    }
}
