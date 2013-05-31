package com.fasterxml.clustermate.service.metrics;

/**
 * Optional extra information that may be provided for
 * {@link OperationMetrics}; currently only applicable
 * for deletions which may get queued.
 */
public class DeferQueueMetrics
{
    public int minLength, maxLength;

    /**
     * Number of queued deletions (of all types) in deletion queue.
     */
    public int currentLength;
    
    /**
     * Number of entries in queue allowed, before refusing to add deferred
     * deletions (and just do straight blocking).
     */
    public int maxLengthForDefer;

    /**
     * Maximum amount of estimated queue-induced delay we will accept
     * to try a "deferred" (non-blocking) deletion (which results in 202 response)
     */
    public int delayTargetMsecs;
    
    /**
     * Estimated per-operation delay, in milliseconds
     */
    public double estimatedDelayMsecs;
}
