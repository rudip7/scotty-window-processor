package Synopsis.Wavelets;

/**
 * FrontlineNode Class which is essentially stores averages and information to DataNodes / other Frontline Nodes
 */
public class FrontlineNode {
    DataNode hangChild;     // pointer to DataNode hanging from this
    double value;
    FrontlineNode next;     // pointer to next (upper) fnode
    FrontlineNode prev;     // ppinter to previous (lower) fnode
    double positiveerror;   // error quantities from deleted orphans
    double negativeerror;   // error quantities from deleted orphans
    boolean errorhanging;
    int level;
}
