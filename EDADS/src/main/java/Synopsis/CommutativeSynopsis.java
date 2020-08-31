package Synopsis;

import java.io.Serializable;

public interface CommutativeSynopsis<T> extends MergeableSynopsis<T>, Serializable {
    /**
     * Function to Merge two Synopses.
     *
     * @param other synopsis to be merged with
     * @return merged synopsis
     * @throws Exception
     */
    @Override
    CommutativeSynopsis<T> merge(MergeableSynopsis<T> other);
}
