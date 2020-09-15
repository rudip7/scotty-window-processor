package Synopsis;

import java.io.Serializable;

public class StratifiedSynopsisWrapper<K extends Serializable, S extends Synopsis> implements Serializable{
    final K key; // Stratification key value
    final S synopsis; //

    /**
     * the constructor - set synopsis and its key.
     *
     * @param key
     * @param synopsis
     */

    public StratifiedSynopsisWrapper(K key, S synopsis) {
        this.synopsis = synopsis;
        this.key = key;
    }

    /**
     * @return synopsis
     */
    public S getSynopsis() {
        return synopsis;
    }

    /**
     * @return key
     */
    public K getKey() {
        return key;
    }
}
