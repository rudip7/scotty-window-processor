package Synopsis.Wavelets;

import java.io.Serializable;

public class DataNode implements Serializable {
    double data;
    transient double mostpositiveerrorleft;
    transient double mostnegativeerrorleft;
    transient double mostpositiveerrorright;
    transient double mostnegativeerrorright;
    transient double maxabserror;
    transient int index;
    int level;
    int orderinlevel;
    transient PointerNode hanged;
    transient DataNode parent;
    Utils.relationship reltoparent;
    DataNode leftChild;
    DataNode nextSibling;
    DataNode previousSibling;
    transient HeapNode heap;

}
