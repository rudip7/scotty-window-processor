package Synopsis.Wavelets;

import java.io.Serializable;

public class PointerNode implements Serializable {
    DataNode hangChild;
    PointerNode next;
    double storedvalue;
    double positiveerror;
    double negativeerror;
    boolean errorhanging;
    int level;
    int orderinlevel;
    Utils.relationship reltoparenterrors;
}
