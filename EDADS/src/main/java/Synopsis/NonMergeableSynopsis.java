package Synopsis;

import java.util.ArrayList;

public abstract class NonMergeableSynopsis<Input, T extends Synopsis<Input>> implements Synopsis<Input>{
    private ArrayList<T> combinedSynopses;
    private int nextIndex;

    public abstract int getSynopsisIndex(int streamIndex);

    @Override
    public void update(Input element){
        if (!combinedSynopses.isEmpty()){
            nextIndex = combinedSynopses.size()-1;
            combinedSynopses.get(nextIndex).update(element);
        }
    }
}
