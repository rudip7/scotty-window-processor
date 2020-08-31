package Synopsis.Wavelets;

import Synopsis.NonMergeableSynopsisManager;
import Synopsis.StratifiedSynopsis;
import java.util.ArrayList;


public class SliceWaveletsManager<Input> extends NonMergeableSynopsisManager<Input,WaveletSynopsis<Input>> {

    int slicesPerWindow;
    ArrayList<Integer> sliceStartIndices;

    /**
     * SliceWaveletsManager constructor - assign slices and store the start indices of slices in an Arraylist.
     *
     * @param unifiedSynopses an array of WaveletSynopsis
     */

    public SliceWaveletsManager(ArrayList<WaveletSynopsis<Input>> unifiedSynopses) {
        this.unifiedSynopses = unifiedSynopses;
        this.slicesPerWindow = unifiedSynopses.size();
        sliceStartIndices = new ArrayList<>(slicesPerWindow);

        elementsProcessed = 0;
        for (int i = 0; i < slicesPerWindow; i++) {
            sliceStartIndices.add(i, elementsProcessed);

            elementsProcessed += unifiedSynopses.get(i).getStreamElementCounter();
        }
    }

    public SliceWaveletsManager() {
        super();
        sliceStartIndices = new ArrayList<>();
    }

    /**
     * Update the structure with a new incoming element by updating last WaveletSynopsis contained in SliceWaveletManager.
     *
     * @param element new incoming element
     */
    @Override
    public void update(Object element) {
        if (!unifiedSynopses.isEmpty()) {
            unifiedSynopses.get(unifiedSynopses.size() - 1).update((Input) element);
        }
    }


    /**
     * return the index of the slice contains streamIndex
     *
     * @param streamIndex
     */
    @Override
    public int getSynopsisIndex(int streamIndex) {
        int index = -1;
        for (int i = 0; i < sliceStartIndices.size(); i++) {
            if (sliceStartIndices.get(i) > streamIndex) {
                return index;
            }
            index++;
        }
        return index;
    }

    /**
     * add a WaveletSynopsis as a new slice to SliceWaveletManager
     *
     * @param synopsis
     */
    @Override
    public void addSynopsis(WaveletSynopsis<Input> synopsis) {
        if (sliceStartIndices == null){
            sliceStartIndices = new ArrayList<>();
        }
        slicesPerWindow++;
        elementsProcessed += synopsis.getStreamElementCounter();
        if (unifiedSynopses.isEmpty()) {
            sliceStartIndices.add(0);
        } else {
            sliceStartIndices.add(sliceStartIndices.get(sliceStartIndices.size() - 1) + unifiedSynopses.get(unifiedSynopses.size() - 1).getStreamElementCounter());
        }
        super.addSynopsis(synopsis);
    }

    /**
     * integrate another NonMergeableSynopsisManager with this object
     *
     * @param other
     */
    @Override
    public void unify(NonMergeableSynopsisManager other) {
//        if (other instanceof SliceWaveletsManager){
            SliceWaveletsManager o = (SliceWaveletsManager) other;
            for (int i = 0; i < o.getUnifiedSynopses().size(); i++) {
                this.addSynopsis((WaveletSynopsis<Input>) o.getUnifiedSynopses().get(i));
            }
//        }
//        Environment.out.println(other.getClass());
//        throw new IllegalArgumentException("It is only possible to unify two objects of type NonMergeableSynopsisManager with each other.");
    }


    /**
     * perform a simple point query based on the given index
     *
     * @param index
     * @return value of the stream element at given index
     */
    public double pointQuery(int index) {
        int managerIndex = getSynopsisIndex(index);
        int previousSliceElements = sliceStartIndices.get(managerIndex);
        return unifiedSynopses.get(managerIndex).pointQuery(index - previousSliceElements);
    }

    /**
     * performs a range sum query.
     *
     * @param leftIndex
     * @param rightIndex
     * @return approximated sum of values between leftIndex and rightIndex
     */
    public double rangeSumQuery(int leftIndex, int rightIndex) {
        int leftManagerIndex = getSynopsisIndex(leftIndex);
        int rightManagerIndex = getSynopsisIndex(rightIndex);

        double rangeSum = 0;

        for (int i = leftManagerIndex; i <= rightManagerIndex; i++) {
            int previousSliceElements = sliceStartIndices.get(i);
            int localLeftIndex = i == leftManagerIndex ? leftIndex - previousSliceElements : 0;
            int localRightIndex = i == rightManagerIndex ? rightIndex - previousSliceElements : sliceStartIndices.get(i + 1) - previousSliceElements - 1;
            rangeSum += unifiedSynopses.get(i).rangeSumQuery(localLeftIndex, localRightIndex);
        }
        return rangeSum;
    }
}
