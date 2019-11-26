package Synopsis;

import java.util.ArrayList;

public abstract class NonMergeableSynopsis<Input, T extends Synopsis<Input>> implements Synopsis<Input>{
    private ArrayList<T> combinedSynopses;

    public abstract int getSynopsisIndex(int streamIndex);

    @Override
    public void update(Input element){
        if (!combinedSynopses.isEmpty()){
            combinedSynopses.get(combinedSynopses.size()-1).update(element);
        }
    }
}
