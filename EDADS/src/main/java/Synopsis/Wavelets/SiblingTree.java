package Synopsis.Wavelets;


import java.util.TreeSet;

public class SiblingTree {

    private int nodecounter;
    private int size;
    private FrontlineNode frontlineBottom;
    private FrontlineNode frontlineTop;
    private DataNode highest;
    private DataNode rootnode;  // only set after the whole data stream is read (in padding)
    private int streamElementCounter;
    private TreeSet<DataNode> errorHeap;


    /**
     * SiblingTree constructor - creates the sibling tree with a given space budget (size).
     *
     * @param size denotes the size budget of the SiblingTree structure which equals the maximum amount of Coefficients
     *             the SiblingTree stores at all times.
     */
    public SiblingTree(int size) {
        this.size = size;
        streamElementCounter = 0;
        nodecounter = 0;

        frontlineBottom = null;
        frontlineTop = null;
        errorHeap = new TreeSet<>();
    }

    public void climbup(double data1, double data2) {

        FrontlineNode frontlineNode = frontlineBottom;
        FrontlineNode prevFrontlineNode = null;
        streamElementCounter += 2;

        int order = streamElementCounter;
        double curentAverage = 0;
        double average = 0;
        int level = 0;
        double value;
        boolean firstLoop = true;

        // loop through the levels from bottom to top and merge the smallest unconnected subtrees until there are a maximum of 1 frontline node per level
        while (order > 0 && order % 2 == 0) {
            DataNode child = null;
            DataNode sibling = null;
            order /= 2;
            level++;

            if (firstLoop) { //first loop / level 0
                average = (data1 + data2) / 2;
                value = data1 - average;
                firstLoop = false;
            } else {
                average = (average + curentAverage) / 2;
                value = curentAverage - average;
                child = prevFrontlineNode.hungChild;
                prevFrontlineNode.hungChild = null;
            }

            if (frontlineNode != null && frontlineNode.level == level) {
                sibling = frontlineNode.hungChild;
                while (sibling.nextSibling != null) {
                    sibling = sibling.nextSibling;          // set s to be the last sibling of the hung child of f
                }
            }

            DataNode current = new DataNode(value, level, order, child, sibling);   // create new DataNode with computed values and bidirectional references to child and sibling


            // TODO: compute error values for current from children, fnode below
            current.computeErrorValues(prevFrontlineNode);

            current.computeMA();        // compute the maximum absolute error of the new node
            errorHeap.add(current);     // add the new node to the error Heap structure

            if (frontlineNode != null && frontlineNode.prev != null){
                frontlineNode.prev = null;      // delete the previous fnode
            }

            FrontlineNode newFrontlineNode = frontlineNode;

            if (frontlineNode == null){     // this is only the case if the new frontline node is the highest frontline-node
                newFrontlineNode = new FrontlineNode(average, level);
                frontlineTop = newFrontlineNode;
                frontlineBottom = newFrontlineNode;
            }else if (frontlineNode.level != level) {   // this is the case when a new frontline node is created but there are still other frontline nodes with higher levels in the structure
                newFrontlineNode = new FrontlineNode(average, level);
                frontlineBottom = newFrontlineNode;
                newFrontlineNode.next = frontlineNode;
                frontlineNode.prev = newFrontlineNode;
            } else {
                curentAverage = frontlineNode.value;
            }

            if (newFrontlineNode.hungChild == null) {
                newFrontlineNode.hungChild = current;
                current.setFrontLineForDescendants(newFrontlineNode);
            }
            prevFrontlineNode = frontlineNode;
            frontlineNode = newFrontlineNode.next;
        }
    }

    @Override
    public String toString() {
        String s = "";
        if (frontlineBottom == null) {
            return "The Sibling Tree is empty.";
        } else {
            FrontlineNode current = frontlineTop;
            while (current != null) {
                s += (current.toString() + ":\n");
                if (current.hungChild != null) {
                    s += (current.hungChild.toString() + "\n");
                }
                s += "----------------------------------------------------------\n";
                current = current.prev;
            }

            return s;
        }
    }
}
