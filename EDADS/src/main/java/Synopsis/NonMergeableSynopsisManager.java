package Synopsis;

import java.util.ArrayList;

public abstract class NonMergeableSynopsisManager<Input, S extends Synopsis<Input>> extends StratifiedSynopsis implements Synopsis<Input>{
    protected ArrayList<S> unifiedSynopses;
    protected int elementsProcessed = 0;

    public NonMergeableSynopsisManager(){
        unifiedSynopses = new ArrayList<>();
    }

    /**
     * return the index of the synopsis contains streamIndex
     *
     * @param streamIndex
     */
    public abstract int getSynopsisIndex(int streamIndex);

    /**
     * add new synopsis to a collection of synopses
     *
     * @param synopsis
     */
    public void addSynopsis(S synopsis){
        unifiedSynopses.add(synopsis);
    }

    public ArrayList<S> getUnifiedSynopses() {
        return unifiedSynopses;
    }

    public int getElementsProcessed() {
        return elementsProcessed;
    }

    /**
     * unify tow noneNonMergeable Synopses
     *
     * @param other
     */
    public void unify(NonMergeableSynopsisManager other){
        elementsProcessed += other.getElementsProcessed();
        unifiedSynopses.addAll(other.getUnifiedSynopses());
    }

    /**
     * empty the NonMergeableSynopsis manager
     */
    public void cleanManager(){
        unifiedSynopses = new ArrayList<>();
        elementsProcessed = 0;
    }
}
