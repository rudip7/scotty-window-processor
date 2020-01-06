package Synopsis.Wavelets;

import Synopsis.NonMergeableSynopsis;

import java.util.ArrayList;

public class SliceWaveletsManager<Input> extends NonMergeableSynopsis<Input, DistributedWaveletsManager<Input>> {

    ArrayList<DistributedWaveletsManager<Input>> slices;
    int slicesPerWindow;
    int[] sliceStartIndices;

    public SliceWaveletsManager(ArrayList<DistributedWaveletsManager<Input>> slices, int slicesPerWindow) {
        this.slices = slices;
        this.slicesPerWindow = slicesPerWindow;
        sliceStartIndices = new int[slicesPerWindow];

        int previousSliceElements = 0;
        for (int i = 0; i < slicesPerWindow; i++) {
            sliceStartIndices[i] = previousSliceElements;

            for (int j = 0; j < slices.get(i).parallelism; j++) {
                previousSliceElements += slices.get(i).getCombinedSynopses().get(j).getStreamElementCounter();
            }
        }
    }

    public SliceWaveletsManager(ArrayList<DistributedWaveletsManager<Input>> slices) {
        this.slices = slices;
        this.slicesPerWindow = slices.size();
        sliceStartIndices = new int[slicesPerWindow];

        int previousSliceElements = 0;
        for (int i = 0; i < slicesPerWindow; i++) {
            sliceStartIndices[i] = previousSliceElements;

            for (int j = 0; j < slices.get(i).parallelism; j++) {
                previousSliceElements += slices.get(i).getCombinedSynopses().get(j).getStreamElementCounter();
            }
        }
    }

    @Override
    public int getSynopsisIndex(int streamIndex) {
        return 0;
    }
}
