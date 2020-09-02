package Synopsis.Sampling;

import Synopsis.MergeableSynopsis;

/**
 * Interface for samplers which do sampling based on time stamp of the samples, it works with TimestampedElement.
 * @param <T>
 */
public interface SamplerWithTimestamps<T> extends MergeableSynopsis<TimestampedElement<T>> {

}
