package Synopsis.Wavelets;

import java.io.Serializable;

/**
 * Class which represents an error-tree node in the sibling tree.
 *
 */
public class DataNode implements Serializable {
    public double data;    // coefficient value
    public double mostpositiveerrorleft;
    public double mostnegativeerrorleft;
    public double mostpositiveerrorright;
    public double mostnegativeerrorright;
    public double maxabserror;
    public int index;  // index of node in the full error-tree (after padding)
    public int level;  // level of node in sibling-tree
    public int orderinlevel;   // order of node in error-tree level
    public FrontlineNode front;    // Frontline node where this node is hanged
    public DataNode leftChild;
    public DataNode parent;
    public Utils.relationship reltoparent;     // relationship of this node to its parent node
    public DataNode nextSibling;
    public DataNode previousSibling;

    public DataNode(double data, int level, int orderinlevel) {
        this.data = data;
        this.level = level;
        this. orderinlevel = orderinlevel;
    }

    protected void computeIndex(int maxLevel){
        index = (int) (Math.pow(2, maxLevel-level)) + orderinlevel - 1;
    }

    @Override
    public String toString() {
        String s = "Coeff. value: "+data;

        if (leftChild != null){
            s+=("\nLeft child: ["+leftChild.toString()+"]");
        }
        if (nextSibling != null){
            s+=("\nSibling: ["+nextSibling.toString()+"]");
        }
        return s;
    }

    public void setFrontLineForDescendants(FrontlineNode frontlineNode){
        front = frontlineNode;
        if (leftChild != null){
            leftChild.setFrontLineForDescendants(frontlineNode);
        }
        if (nextSibling != null){
            nextSibling.setFrontLineForDescendants(frontlineNode);
        }
    }
}
