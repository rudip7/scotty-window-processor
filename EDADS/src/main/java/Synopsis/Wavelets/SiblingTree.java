package Synopsis.Wavelets;

public class SiblingTree {

    int nodecounter;
    private int size;
    FrontlineNode frontlinefirst;
    FrontlineNode highesthangingfrontline;
    DataNode highest;
    DataNode rootnode;

    /**
     * SiblingTree constructor - creates the sibling tree with a given space budget (size).
     *
     * @param size denotes the size budget of the SiblingTree structure which equals the maximum amount of Coefficients
     *             the SiblingTree stores at all times.
     */
    public SiblingTree(int size) {
        this.size = size;

        frontlinefirst = new FrontlineNode(0, 1);
        frontlinefirst.errorhanging = false;
    }


}
