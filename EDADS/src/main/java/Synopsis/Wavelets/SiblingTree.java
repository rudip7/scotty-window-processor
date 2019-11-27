package Synopsis.Wavelets;

import java.util.ArrayList;
import java.util.LinkedList;

public class SiblingTree {

    private int nodecounter;
    private int size;
    private FrontlineNode frontlineBottom;
    private FrontlineNode frontlineTop;
    private DataNode highest;
    private DataNode rootnode;  // only set after the whole data stream is read (in padding)
    private int streamElementCounter;

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
//        frontlineBottom.errorhanging = false;
    }

    public void climbup(double data1, double data2) {

        FrontlineNode frontlineNode = frontlineBottom;
        FrontlineNode prevFrontlineNode = null;
        streamElementCounter += 2;

        int order = streamElementCounter;
        double curentAverage = Double.MIN_VALUE;
        double average = 0;
        int level = 0;
        double value;

        // loop through the levels from bottom to top and merge the smallest unconnected subtrees until there are a maximum of 1 frontline node per level
        while (order > 0 && order % 2 == 0) {
            DataNode child = null;
            DataNode sibling = null;
            order /= 2;
            level++;

            if (curentAverage == Double.MIN_VALUE) { //first loop / level 0
                average = (data1 + data2) / 2;
                value = data1 - average;
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

            DataNode current = new DataNode(value, level, order);
            current.leftChild = child;
            if (sibling != null) {
                current.previousSibling = sibling;
                sibling.nextSibling = current;
            }

            // TODO: compute error values for current from children, fnode below f

            // TODO: compute MA; put ci in min-heap H;

            if (frontlineBottom == prevFrontlineNode) {
                frontlineBottom = null;
            }
            if (prevFrontlineNode != null && prevFrontlineNode.next != null) {
                prevFrontlineNode.next.prev = prevFrontlineNode.prev;
            }
            if (frontlineNode.prev != null){
                frontlineNode.prev = null;
            }

            FrontlineNode iterate = frontlineBottom;
            while (iterate != null && iterate.next != null) {
                if (iterate.level == level) break;
                iterate = iterate.next;
            }

            if (iterate == null || iterate.level != level) {
                frontlineNode = new FrontlineNode(average, level);
                if (frontlineTop == null || frontlineTop.level < level){
                    frontlineTop = frontlineNode;
                }
                if (frontlineBottom == null || frontlineBottom.level > level) {
                    if (frontlineBottom != null) {
                        frontlineBottom.prev = frontlineNode;
                        frontlineNode.next = frontlineBottom;
                    }
                    frontlineBottom = frontlineNode;
                }
            } else {
                curentAverage = frontlineNode.value;
            }

            if (frontlineNode.hungChild == null) {
                frontlineNode.hungChild = current;
            }
            prevFrontlineNode = frontlineNode;
            frontlineNode = frontlineNode.next;
        }
    }

    @Override
    public String toString() {
        String s = "";
        if (frontlineBottom == null) {
            return "The Sibling Tree is empty.";
        } else {
            FrontlineNode current = frontlineBottom;
            while (current != null) {
                s += (current.toString() + ":\n");
                if (current.hungChild != null) {
                    s += (current.hungChild.toString() + "\n");
                }
                s += "----------------------------------------------------------\n";
                current = current.next;
            }

            return s;
        }
    }
}
