package com.fasterxml.clustermate.service.cfg;

/**
 * Configuration container used for defining if and how certain types
 * of operations can be deferred (that is, handled asynchronously
 * and possibly after returning control to caller).
 */
public class DeferredOperationConfig
{
    /**
     * Is deferral of this operation allowed?
     */
    public boolean allowDeferred;

    /**
     * After what size should we be concerned about queuing, and
     * (possibly) log a warning?
     */
    public int deferredQueueWarnSize;

    /**
     * Beyond level of {@link #deferredQueueWarnSize}, when should we actual
     * start taking more drastic actions, like shedding operations (dropping)?
     * Used as a multiplier for {@link #deferredQueueWarnSize} to determine
     * <code>maximum length</code> for queue.
     *<p>
     * NOTE: exact semantics may vary between operations, and code that implements
     * limits -- this may be soft or hard limit; and implementation may use shedding,
     * blocking, synchronous operations or some combination that it sees fit.
     */
    public double deferredQueueMaxMultiplier;
    
    public DeferredOperationConfig() {
        this(true, 1000, 2.0);
    }

    public DeferredOperationConfig(boolean allow,
            int warnSize, double maxMultiplier) {
        allowDeferred = allow;
        deferredQueueWarnSize = warnSize;
        
    }
}
