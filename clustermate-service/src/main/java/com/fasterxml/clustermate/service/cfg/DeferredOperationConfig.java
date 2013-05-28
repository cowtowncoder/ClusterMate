package com.fasterxml.clustermate.service.cfg;

/**
 * Configuration container used for defining if and how certain types
 * of operations can be deferred (that is, handled asynchronously
 * and possibly after returning control to caller).
 */
public class DeferredOperationConfig
{
    /**
     * Is deferral of this operation allowed? Default value of null
     * means "use store default", meaning of which is implementation-dependant.
     */
    public Boolean allowDeferred = null;

    /**
     * This is the size of deferred-operation queue that is considered normal,
     * and no additional processing (warning, delaying) is taken if size remains
     * below threshold.
     */
    public int queueOkSize;

    /**
     * This is the size of deferred-operation queue that is considered long enough
     * so that callers should be delayed by adding bit of delay; but operations
     * will be normally queued without shedding.
     */
    public int queueRetainSize;

    /**
     * This is the size of deferred-operation queue that is considered too long,
     * ad entries will be automatically dropped if size is exceeded. In addition,
     * entries beyond {@link #queueRetainSize} <b>may</b> be dropped (likelihood
     * being directly related to how close we are to this limit); and if not dropped,
     * caller will be delayed by maximum allowed amount.
     */
    public int queueMaxSize;

    /**
     * If callers are to be delayed (when queue size exceeds {@link #queueOkSize}, this
     * is the minimum delay that may be imposed.
     */
    public long callerMinDelayMsecs;
    
    /**
     * If callers are to be delayed (when queue size exceeds {@link #queueOkSize}, this
     * is maximum delay that may be imposed.
     */
    public long callerMaxDelayMsecs;
    
    /**
     * Default constructor that allows use of deferral, and uses queue lengths of
     * 1000, 2000 and 4000 (for ok, retain and
     * max sizes); and delay between 10 and 100 milliseconds.
     */
    public DeferredOperationConfig() {
        this(true, 1000, 2000, 4000, 10L, 100L);
    }

    public DeferredOperationConfig(Boolean allow,
            int okSize, int retainSize, int maxSize,
            long minDelayMsecs, long maxDelayMsecs)
    {
        allowDeferred = allow;
        queueOkSize = okSize;
        queueRetainSize = retainSize;
        queueMaxSize = maxSize;
        callerMinDelayMsecs = minDelayMsecs;
        callerMaxDelayMsecs = maxDelayMsecs;
    }

    public boolean allowDeferred(boolean defaultState)
    {
        if (allowDeferred == null) {
            return defaultState;
        }
        return allowDeferred.booleanValue();
    }
}
