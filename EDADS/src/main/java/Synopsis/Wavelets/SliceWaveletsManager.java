package Synopsis.Wavelets;

import Synopsis.NonMergeableSynopsis;

import java.util.ArrayList;

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

    public SliceWaveletsManager(ArrayList<WaveletSynopsis<Input>> slices) {
        this.slices = slices;
        this.slicesPerWindow = slices.size();
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
        //TODO
        return 0;
    }

    public double rangeSumQuery(int leftIndex, int rightIndex){
        //TODO
        return 0;
    }
}
