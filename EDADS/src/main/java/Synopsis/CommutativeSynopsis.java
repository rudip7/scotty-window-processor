package Synopsis;

import java.io.Serializable;

public interface CommutativeSynopsis<T> extends Synopsis<T>, Serializable {
    @Override
    CommutativeSynopsis<T> merge(Synopsis<T> other);
}
