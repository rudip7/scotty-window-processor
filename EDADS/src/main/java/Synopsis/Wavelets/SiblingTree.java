package Synopsis.Wavelets;


import java.util.PriorityQueue;

public class SiblingTree {

    private int nodecounter = 0;
    private int size;
    private FrontlineNode frontlineBottom;
    private FrontlineNode frontlineTop;
    private DataNode rootnode;  // only set after the whole data stream is read (in padding)
    private int streamElementCounter;
    private PriorityQueue<DataNode> errorHeap;


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
        errorHeap = new PriorityQueue<>();
    }

    public void climbup(double data1, double data2) {

        FrontlineNode frontlineNode = frontlineBottom;
        FrontlineNode prevFrontlineNode = null;
        streamElementCounter += 2;
        nodecounter += 2; // every climbub operation increases the amount of (Data and Frontline) nodes by 2

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


            current.computeErrorValues(prevFrontlineNode);      // compute the error values for the new DataNode from children and the previous frontline node
            current.computeMA();        // compute the maximum absolute error of the new node
            errorHeap.add(current);     // add the new node to the error Heap structure


            // delete the previous frontline node by removing all of its references
            if (prevFrontlineNode != null){
                if (child != null){
                    child.front = null;             // remove the reference of the hung child to the previous frontline node
                }
                if (frontlineNode != null){
                    frontlineNode.prev = null;      // remove the reference of the frontline node to the previous frontline node
                }
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

            if (newFrontlineNode.hungChild == null) {       // hang the newly created DataNode to the new Frontline node (only if new frontline node was actually created)
                newFrontlineNode.hungChild = current;
                current.front = newFrontlineNode;
            }
            prevFrontlineNode = frontlineNode;
            frontlineNode = newFrontlineNode.next;
        }
    }

    /**
     * function which discards the DataNode from the Heap which incurs the smallest absolute error
     */
    public void discard(){
        DataNode discarded = errorHeap.poll();

        propagateError(discarded);

        if (discarded.leftMostChild != null){   // handle children / siblings
            DataNode child = discarded.leftMostChild;

            if (discarded.front != null){       // hang child on frontline in place of discarded node
                child.front = discarded.front;
                discarded.front.hungChild = child;
            }

            if (discarded.previousSibling != null){     // connect child as right sibling of previous sibling of the discarded node
                discarded.previousSibling.nextSibling = child;
                child.previousSibling = discarded.previousSibling;
            }
            if (discarded.nextSibling != null){     // connect last sibling of child as left sibling of discarded.next
                while (child.nextSibling != null){
                    child = child.nextSibling;
                }
                child.nextSibling = discarded.nextSibling;
                discarded.nextSibling.previousSibling = child;
            }
        }else {     // no child
            if (discarded.front != null){
                if (discarded.nextSibling != null){
                    discarded.nextSibling.front = discarded.front;
                }
                discarded.front.hungChild = discarded.nextSibling;
            }
            if (discarded.previousSibling != null){
                discarded.previousSibling.nextSibling = discarded.nextSibling;
            }
            if (discarded.nextSibling != null){
                discarded.nextSibling.previousSibling = discarded.previousSibling;
            }
        }
        if (discarded.parent != null){  // handle parent
            if (discarded.leftMostChild != null){
                discarded.leftMostChild.parent = discarded.parent;
                discarded.parent.leftMostChild = discarded.leftMostChild;
            }else {
                if (discarded.nextSibling != null){
                    discarded.nextSibling.parent = discarded.parent;
                }
                discarded.parent.leftMostChild = discarded.nextSibling;
            }
        }
    }

    /**
     * method which takes care of the error propagation when discarding a node
     * @param discarded     data node to be discarded
     */
    private void propagateError(DataNode discarded){
        discarded.minerrorleft -= discarded.data;
        discarded.maxerrorleft -= discarded.data;
        discarded.minerrorright += discarded.data;
        discarded.maxerrorright += discarded.data;


        if (discarded.leftMostChild != null){
            propagateErrorDown(discarded.leftMostChild, discarded);
        }
        if (discarded.parent == null){
            double minError = Math.min(discarded.minerrorleft, discarded.minerrorright);
            double maxError = Math.max(discarded.maxerrorleft, discarded.maxerrorright);
            if (discarded.front == null){       // store/merge error in fnode of ck's leftmost sibling
                DataNode sibling = discarded.previousSibling;
                while (sibling.previousSibling != null){
                    sibling = sibling.previousSibling;
                }
                sibling.front.mergeError(minError, maxError);
            }else {     // store/merge error in fnode
                discarded.front.mergeError(minError, maxError);
            }
        }else {     // parents exist
            propagateErrorUp(discarded.parent);
        }
    }

    /**
     * propagates error up as long as necessary
     * @param parent
     */
    private void propagateErrorUp(DataNode parent){
        boolean propagateUpNecessary = true;
        while(propagateUpNecessary && parent != null){
            propagateUpNecessary = parent.computeErrorValues(null);
            parent = parent.parent;
        }
    }

    /**
     * propagates error of deleted node down
     * @param descendant    descendant of deleted node
     * @param ancestor      node to be deleted
     */
    private void propagateErrorDown(DataNode descendant, DataNode ancestor){
        errorHeap.remove(descendant);   // remove the descendant from the error heap

        if (descendant.ancestorRelationship(ancestor) == Utils.relationship.leftChild){     // decrease all error measures in left subtree of ancestor
            descendant.minerrorleft -= ancestor.data;
            descendant.maxerrorleft -= ancestor.data;
            descendant.minerrorright -= ancestor.data;
            descendant.maxerrorright -= ancestor.data;
        }else{      // increase all error measures in right subtree of ancestor
            descendant.minerrorleft += ancestor.data;
            descendant.maxerrorleft += ancestor.data;
            descendant.minerrorright += ancestor.data;
            descendant.maxerrorright += ancestor.data;
        }
        descendant.computeMA();
        errorHeap.add(descendant);      // add descendant to error heap with recomputed value
        if (descendant.leftMostChild != null){
            propagateErrorDown(descendant.leftMostChild, ancestor);     // propagate error to all children
        }
        if (descendant.nextSibling != null){
            propagateErrorDown(descendant.nextSibling, ancestor);       // propagate error to all siblings
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
