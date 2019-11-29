package Synopsis.Wavelets;

/**
 * FrontlineNode Class which is essentially stores averages and information to DataNodes / other Frontline Nodes
 */
public class FrontlineNode {


    public DataNode hungChild;     // pointer to DataNode hanging from this
    public double value;
    public FrontlineNode next;     // pointer to next (upper) fnode
    public FrontlineNode prev;     // ppinter to previous (lower) fnode
    public double positiveerror;   // error quantities from deleted orphans
    public double negativeerror;   // error quantities from deleted orphans
    public boolean errorhanging;
    public int level;

    public FrontlineNode(int level) {
        this.level = level;
        errorhanging = false;
    }

    public FrontlineNode(double value, int level) {
        this.value = value;
        this.level = level;
        errorhanging = false;
    }

    @Override
    public String toString() {
        return ("Level " + this.level + " ---> " + this.value);
    }
}
