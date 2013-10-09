package com.fasterxml.clustermate.service.cfg;

import java.util.concurrent.TimeUnit;

import org.skife.config.TimeSpan;

/**
 * Configuration container used for defining how delete operations
 * are queued for execution, including possibility of "deferred"
 * (asynchronous) deletion, in which calling request may be served
 * before actual deletion takes place.
 * Only limited number of deletions can be deferred in such a way,
 * since blocking-induced delay is generally needed as flow-control
 * mechanism.
 */
public class DeferredDeleteConfig
{
    /**
     * Default target for delay entries are to spend in queue is 100
     * milliseconds.
     */
    private final static TimeSpan DEFAULT_TARGET_DELAY = new TimeSpan(100, TimeUnit.MILLISECONDS);
    
    /**
     * This is the minimum length of deferred deletion queue to use,
     * regardless of dynamic calculation
     */
    public int minQueueLength;

    /**
     * This is the minimum length of deferred deletion queue to use,
     * regardless of dynamic calculation.
     */
    public int maxQueueLength;

    /**
     * Defines target for maximum delay induced by queue, in milliseconds;
     * used together with estimation of per-operation delay to estimate
     * maximum dynamic queue length to use (with bounds defined by
     * {@link #minQueueLength} and {@link #maxQueueLength}).
     *<p>
     * Note that specifying value of 0 (or negative) will effectively
     * disable use of deferred-deletes queue.
     *<p>
     * Default value is 100 milliseconds
     */
    public TimeSpan queueTargetDelayMsecs;

    /**
     * Maximum delay for entries (whether deferred or blocked) before
     * request will result in time-out failure.
     *<p>
     * Default value is 2500 milliseconds.
     */
    public TimeSpan queueMaxDelayMsecs;

    public DeferredDeleteConfig() {
        this(5, 100, DEFAULT_TARGET_DELAY,
                new TimeSpan(2500, TimeUnit.MILLISECONDS)
                // !!! TEST
//        new TimeSpan(1000, TimeUnit.MILLISECONDS)
            );
    }
    
    public DeferredDeleteConfig(int minQueueLength, int maxQueueLength,
            TimeSpan queueTargetDelayMsecs, TimeSpan queueMaxDelayMsecs)
    {
        if (minQueueLength > maxQueueLength) {
            throw new IllegalArgumentException("minQueueLength ("+minQueueLength+") can not exceed maxQueueLength ("+maxQueueLength+")");
        }
        this.minQueueLength = minQueueLength;
        this.maxQueueLength = maxQueueLength;
        if (queueTargetDelayMsecs.getMillis() > queueMaxDelayMsecs.getMillis()) {
            throw new IllegalArgumentException("queueTargetDelayMsecs ("+queueTargetDelayMsecs+") can not exceed queueMaxDelayMsecs ("+queueMaxDelayMsecs+")");
        }
        this.queueTargetDelayMsecs = queueTargetDelayMsecs;
        this.queueMaxDelayMsecs = queueMaxDelayMsecs;
    }
}
