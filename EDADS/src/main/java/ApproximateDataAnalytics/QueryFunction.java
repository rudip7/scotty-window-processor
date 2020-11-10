package ApproximateDataAnalytics;

import Synopsis.Synopsis;

import java.io.Serializable;

/**
 * Interface for querying a sketch
 *
 * @author Joscha von Hein
 *
 * @param <S>   synopsis
 * @param <Q>   queryInput
 * @param <O>   queryOutput
 */
public interface QueryFunction<Q, S extends Synopsis, O> extends Serializable {

    public O query (Q query, S synopsis);
}
