package Synopsis.Wavelets;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

/**
 * Class which represents an error-tree node in the sibling tree.
 *
 */
public class DataNode implements Serializable, Comparable<DataNode> {
    public double data;    // coefficient value
    public double maxerrorleft;     // maximum error left subtree
    public double minerrorleft;     // minimum error left subtree
    public double maxerrorright;     // maximum error right subtree
    public double minerrorright;     // minimum error right subtree
    public double maxabserror;
    public int index;  // index of node in the full error-tree (after padding)
    public int level;  // level of node in sibling-tree
    public int orderinlevel;   // order of node in error-tree level
    public FrontlineNode front;    // Frontline node where this node is hanged
    public DataNode leftMostChild;  // this nodes leftmost child - usually on this nodes left subtree but can in cases of multiple deletions also be on the right subtree
    public DataNode parent;
    public Utils.relationship reltoparent;     // relationship of this node to its parent node
    public DataNode nextSibling;
    public DataNode previousSibling;

    public DataNode(double data, int level, int orderinlevel, DataNode leftChild, DataNode previousSibling) {
        this.data = data;
        this.level = level;
        this.orderinlevel = orderinlevel;
        if (leftChild != null){
            this.leftMostChild = leftChild;
            this.leftMostChild.setParent(this);
        }
        if (previousSibling != null){
            this.previousSibling = previousSibling;
            previousSibling.nextSibling = this;
        }
        reltoparent = Utils.relationship.none;
    }

    public void computeIndex(int maxLevel){
        index = (int) (Math.pow(2, maxLevel-level)) + orderinlevel - 1;
    }

    public void computeErrorValues(FrontlineNode prevFrontlineNode){

        DataNode currentChild = leftMostChild;

        while (currentChild != null){
            double maxerror = Math.max(currentChild.maxerrorleft, currentChild.maxerrorright);
            double minerror = Math.min(currentChild.minerrorleft, currentChild.minerrorright);
            if (currentChild.reltoparent == Utils.relationship.leftChild){
                maxerrorleft = Math.max(maxerrorleft, maxerror);
                minerrorleft = Math.min(minerrorleft, minerror);
            }else if (currentChild.reltoparent == Utils.relationship.rightChild){
                maxerrorright = Math.max(maxerrorright, maxerror);
                minerrorright = Math.min(minerrorright, minerror);
            }

            currentChild = currentChild.nextSibling;
        }

        if (prevFrontlineNode != null && prevFrontlineNode.errorhanging){
            maxerrorleft = Math.max(maxerrorleft, prevFrontlineNode.positiveerror);
            minerrorleft = Math.min(minerrorleft, prevFrontlineNode.negativeerror);
        }
    }


    /**
     * compute the potential maximum absolute error if this node were to be discarded
     * @return      potential maximum absolute error
     */
    public double computeMA(){
        double left = Math.max(Math.abs(maxerrorleft - data), Math.abs(minerrorleft - data));
        double right = Math.max(Math.abs(maxerrorright + data), Math.abs(minerrorright + data));
        maxabserror =  Math.max(left,right);
        return maxabserror;
    }

    @Override
    public String toString() {
        String s = "Coeff. value: "+data;

        if (leftMostChild != null){
            s+=("\nLeft child: ["+ leftMostChild.toString()+"]");
        }
        if (nextSibling != null){
            s+=("\nSibling: ["+nextSibling.toString()+"]");
        }
        return s;
    }

    public void setFrontLineForDescendants(FrontlineNode frontlineNode){
        front = frontlineNode;
        if (leftMostChild != null){
            leftMostChild.setFrontLineForDescendants(frontlineNode);
        }
        if (nextSibling != null){
            nextSibling.setFrontLineForDescendants(frontlineNode);
        }
    }

    /**
     * sets the parent and also computes it's relation to it
     * @param parent
     */
    public void setParent(DataNode parent){
        this.parent = parent;
        if (parent != null){
            if (parent.level > this.level){
                if ((2 * orderinlevel - 1) < ((2 * parent.orderinlevel -1) * Math.pow(2, parent.level - this.level))){
                    reltoparent = Utils.relationship.leftChild;
                }else {
                    reltoparent = Utils.relationship.rightChild;
                }
            }
        }else {
            reltoparent = Utils.relationship.none;
        }
    }

    /**
     * Compare to method based on the maximum absolute error.
     * If two DataNodes have the same maxabserror the level and subsequently order in level get compared instead.
     * This way no two DataNodes in the same SiblinTree should ever be equal!
     *
     * This method specifically compares the error so the DataNodes can be stored in a convenient Sorted Structure based on error
     * (so the nodes with the lowest potential absolute error can be deleted first).
     *
     * @param o     DataNode to compare To
     * @return      -1 if this is smaller, 0 if equal, 1 if bigger than other DataNode (in regards to maximum absolute error)
     */
    @Override
    public int compareTo(@NotNull DataNode o) {
        if (maxabserror == o.maxabserror){
            if (level == o.level){
                return Integer.compare(orderinlevel, o.orderinlevel);
            }
            return Integer.compare(level, o.level);
        }
        return Double.compare(maxabserror, o.maxabserror);
    }

    /**
     * equals method consistent with compareTo() method so no problems arise when a DataNode is put into an ordered map
     * with MA as key.
     *
     * @param obj   Object to compare to
     * @return      true if obejcts are equal
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DataNode){
            DataNode o = (DataNode) obj;
            if (o.level == level && o.orderinlevel == orderinlevel && o.maxabserror == maxabserror){
                return true;
            }
        }
        return false;
    }


}
