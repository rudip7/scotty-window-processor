package Synopsis;

import java.util.ArrayList;

public abstract class NonMergeableSynopsisManager<T extends Synopsis> extends StratifiedSynopsis{
    protected ArrayList<T> unifiedSynopses;

    public NonMergeableSynopsisManager(){
        unifiedSynopses = new ArrayList<>();
    }

    public abstract int getSynopsisIndex(int streamIndex);

    public void addSynopsis(T synopsis){
        unifiedSynopses.add(synopsis);
    }

    public ArrayList<T> getUnifiedSynopses() {
        return unifiedSynopses;
    }

    public void unify(NonMergeableSynopsisManager other){
        unifiedSynopses.addAll(other.getUnifiedSynopses());
    }
}
