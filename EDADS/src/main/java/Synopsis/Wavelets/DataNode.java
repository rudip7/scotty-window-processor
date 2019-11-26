package Synopsis.Wavelets;

import java.io.Serializable;

/**
 * Class which represents an error-tree node in the sibling tree.
 *
 */
public class DataNode implements Serializable {
    double data;    // coefficient value
    double mostpositiveerrorleft;
    double mostnegativeerrorleft;
    double mostpositiveerrorright;
    double mostnegativeerrorright;
    double maxabserror;
    int index;  // index of node in the full error-tree (after padding)
    int level;  // level of node in sibling-tree
    int orderinlevel;   // order of node in error-tree level
    FrontlineNode front;    // Frontline node where this node is hanged
    DataNode leftChild;
    DataNode parent;
    Utils.relationship reltoparent;     // relationship of this node to its parent node
    DataNode nextSibling;
    DataNode previousSibling;

    public DataNode(double data, int level, int orderinlevel) {
        this.data = data;
        this.level = level;
        this. orderinlevel = orderinlevel;
    }

    protected void computeIndex(int maxLevel){
        index = (int) (Math.pow(2, maxLevel-level)) + orderinlevel - 1;
    }
}
