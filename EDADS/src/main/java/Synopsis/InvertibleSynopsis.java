package Synopsis;

import java.io.Serializable;

public interface InvertibleSynopsis<T> extends CommutativeSynopsis<T>, Serializable {
    InvertibleSynopsis<T> invert(InvertibleSynopsis<T> toRemove);

    /**
     * Remove an element from synopsis.
     *
     * @param toDecrement synopsis to be removed.
     */
    void decrement(T toDecrement);

    /**
     * Function to Merge two Synopses.
     *
     * @param other synopsis to be merged with
     * @return merged synopsis
     * @throws Exception
     */
    @Override
    InvertibleSynopsis<T> merge(MergeableSynopsis<T> other);

}
