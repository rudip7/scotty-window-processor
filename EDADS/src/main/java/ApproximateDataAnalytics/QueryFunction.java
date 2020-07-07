package ApproximateDataAnalytics;

import Synopsis.Synopsis;

/**
 * Interface for querying a sketch
 *
 * @author Joscha von Hein
 *
 * @param <S>   Sketch
 * @param <Q>   QueryInput
 * @param <O>   QueryOutput
 */
public interface QueryFunction<S extends Synopsis, Q, O> {

    public O query (S sketch, Q query);
}
