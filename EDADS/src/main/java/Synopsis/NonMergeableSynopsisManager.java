package Synopsis;

import java.util.ArrayList;

public abstract class NonMergeableSynopsisManager<Input, S extends Synopsis<Input>> extends StratifiedSynopsis implements Synopsis<Input>{
    protected ArrayList<S> unifiedSynopses;

    public NonMergeableSynopsisManager(){
        unifiedSynopses = new ArrayList<>();
    }

    public abstract int getSynopsisIndex(int streamIndex);

    public void addSynopsis(S synopsis){
        unifiedSynopses.add(synopsis);
    }

    public ArrayList<S> getUnifiedSynopses() {
        return unifiedSynopses;
    }

    public void unify(NonMergeableSynopsisManager other){
        unifiedSynopses.addAll(other.getUnifiedSynopses());
    }
}
