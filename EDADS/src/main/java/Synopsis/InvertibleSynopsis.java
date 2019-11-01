package Synopsis;

import java.io.Serializable;

public interface InvertibleSynopsis<T> extends Synopsis<T>, Serializable {
    InvertibleSynopsis<T> invert(InvertibleSynopsis<T> toRemove);

    void decrement(T toDecrement);

    @Override
    InvertibleSynopsis<T> merge(Synopsis<T> other);

}
