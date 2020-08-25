package Synopsis.Wavelets;

import java.io.Serializable;

/**
 * Class which represents different types of relationship in trees.
 */
public class Utils implements Serializable {
    enum relationship {
        leftChild,
        rightChild,
        none
    }
}