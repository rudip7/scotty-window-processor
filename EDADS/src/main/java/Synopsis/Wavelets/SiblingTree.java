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

        frontlineBottom = new FrontlineNode(0, 1);
        frontlineBottom.errorhanging = false;
    }

    public void climbup(double data1, double data2){

        FrontlineNode frontlineNode = frontlineBottom;
        streamElementCounter += 2;

        int order = streamElementCounter;
        double curentAverage = Double.MIN_VALUE;
        double average = 0;
        int level = 0;
        double value;
        DataNode child = null;
        DataNode sibling = null;

        // loop through the levels from bottom to top and merge the smallest unconnected subtrees until there are a maximum of 1 frontline node per level
        while (order > 0 && order % 2 == 0){
            order /= 2;
            level ++;

            if (curentAverage == Double.MIN_VALUE){ //first loop / level 0
                average = (data1 + data2) / 2;
                value = data1 - average;
            }else {
                average = (average+curentAverage)/2;
                value = curentAverage - average;
                if (frontlineNode.prev != null){
                    child = frontlineNode.prev.hungChild;
                    frontlineNode.prev.hungChild = null;
                }
            }

            if (frontlineNode.level == level){
                sibling = frontlineNode.hungChild;
                while (sibling != null && sibling.nextSibling != null){
                    sibling = sibling.nextSibling;          // set s to be the last sibling of the hung child of f
                }
            }

            DataNode current = new DataNode(value, level, order);
            current.leftChild = child;
            current.previousSibling = sibling;

            // TODO: compute error values for current from children, fnode below f

            // TODO: compute MA; put ci in min-heap H;

            frontlineNode.prev = null;

            FrontlineNode iterate = frontlineBottom;
            while (iterate.next != null){
                if (iterate.level == level) break;
                iterate = iterate.next;
            }

            if (iterate.level != level){
                frontlineNode = new FrontlineNode(average, level);
            } else {
                curentAverage = frontlineNode.value;
            }

            if (frontlineNode.hungChild == null){
                frontlineNode.hungChild = current;
            }
            frontlineNode = frontlineNode.next;
        }
    }

    @Override
    public String toString() {
        String s = "";
        FrontlineNode current = frontlineBottom;
        while (current != null){
            s+= ("Level "+current.level+" ---> "+current.value+":\n"+current.hungChild.toString()+"\n\n");
        }

        return super.toString();
    }
}
