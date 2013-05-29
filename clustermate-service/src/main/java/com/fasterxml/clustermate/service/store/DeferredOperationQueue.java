package com.fasterxml.clustermate.service.store;

import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.skife.config.TimeSpan;

import com.fasterxml.clustermate.service.cfg.DeferredOperationConfig;

/**
 * Helper class we use for determining actions to take with respect
 * to deferred deletions.
 */
public class DeferredOperationQueue<E>
{
    /**
     * Action that as taken by queue
     */
    public enum Action {
        /**
         * Operation was queued as-is, without delay
         */
        PROCEED,
        /**
         * Operation was queued, but additional delay was imposed, to give
         * feedback to caller
         */
        DELAY,
        
        /**
         * Operation was dropped due to capacity limit; delay may have been
         * additionally imposed.
         */
        DROP;
    }

    /**
     * To induce probabilistic drops beyond "delay size", we need a pseudo-random
     * number generator.
     */
    protected final Random _random;
    
    /**
     * Limit at which delay of action is allowed.
     */
    protected final int _delayThreshold;

    /**
     * Limit at which dropping of actions is allowed.
     */
    protected final int _dropThreshold;

    protected final int _maxLength;
    
    protected final int _minDelayMsecs, _maxDelayMsecs;
    
    /**
     * When operations are deferred, we just use a simple synchronized queue
     * for passing them through
     */
    private final ArrayBlockingQueue<E> _queuedOperations;
    
    public DeferredOperationQueue(int optimalSize, int delaySize, int maxSize,
            TimeSpan minDelay, TimeSpan maxDelay)
    {
        // bit of sanity checks for fun
        if (optimalSize > delaySize) {
            throw new IllegalArgumentException("optimalSize ("+optimalSize+") can not exceed delaySize ("+delaySize+")");
        }
        if (delaySize > maxSize) {
            throw new IllegalArgumentException("delaySize ("+delaySize+") can not exceed maxSize ("+maxSize+")");
        }
        
        _delayThreshold = optimalSize;
        _dropThreshold = delaySize;
        _maxLength = maxSize;
        _queuedOperations = new ArrayBlockingQueue<E>(maxSize);
        // let's keep things more repeatable by feeding simple sum of sizes
        _random = new Random(optimalSize + delaySize + maxSize);

        _minDelayMsecs = (int) ((minDelay == null) ? 0L : minDelay.getMillis());
        _maxDelayMsecs = (int) ((maxDelay == null) ? 0L : maxDelay.getMillis());

        if (_minDelayMsecs > _maxDelayMsecs) {
            throw new IllegalArgumentException("minDelay ("+minDelay+") can not exceed maxDelay ("+maxDelay+")");
        }
    }

    public static <E> DeferredOperationQueue<E> forConfig(DeferredOperationConfig config,
            boolean enabledByDefault)
    {
        if (!config.allowDeferred(enabledByDefault)) {
            return null;
        }
        return new DeferredOperationQueue<E>(
                config.queueOkSize, config.queueRetainSize, config.queueMaxSize,
                new TimeSpan(config.callerMinDelayMsecs, TimeUnit.MILLISECONDS),
                new TimeSpan(config.callerMaxDelayMsecs, TimeUnit.MILLISECONDS)
                );
    }
    
    /*
    /**********************************************************************
    /* Public API
    /**********************************************************************
     */

    public Action queueOperation(E operation) throws InterruptedException
    {
        int size = _queuedOperations.size();

        // possibly drop?
        if (_shouldDrop(size+1)) {
            return _handleDrop();
        }
        // otherwise try adding
        if (!_queuedOperations.offer(operation)) { // nope, full
            return _handleDrop();
        }
        // let's see if we are to induce some delay to caller...
        int resultSize = Math.max(_queuedOperations.size(), (size+1));
        // otherwise figure out how much to delay
        int delay = _calcDelay(resultSize);
        if (delay <= 0) {
            return Action.PROCEED;
        }
        Thread.sleep((long) delay);
        return Action.DELAY;
    }
    
    public E unqueueOperationNoBlock() throws InterruptedException {
        return _queuedOperations.poll();
    }

    public E unqueueOperationBlock() throws InterruptedException {
        return _queuedOperations.take();
    }

    public int size() {
        return _queuedOperations.size();
    }
    
    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    /**
     * Helper method that determines whether addition of an action (as anticipated
     * in given resulting queue size) should be prevented and action
     * directly dropped (shedded) or not.
     */
    protected boolean _shouldDrop(int estimatedSize)
    {
        // quick case: below threshold
        int above = estimatedSize - _dropThreshold;
        if (above <= 0) {
            return false;
        }
        if (estimatedSize == _maxLength) { // unlikely we'll match but fast to check so
            return true;
        }
        double dropLikelihood =  ((double) above) / (double) (_maxLength - _dropThreshold);
        /* not sure if syncing is really needed wrt state of java.util.Random;
         * but better safe than sorry
         */
        double rnd;
        
        synchronized (_random) {
            rnd = _random.nextDouble();
        }
        return (rnd <= dropLikelihood);
    }

    protected int _calcDelay(int estimatedSize)
    {
        // below delay induction?
        int above = estimatedSize - _delayThreshold;
        if (above <= 0) {
            return 0;
        }
        // or above variability?
        if (estimatedSize >= _dropThreshold) { // at "maybe drop" zone; add max delay
            return _maxDelayMsecs;
        }
        // no, calculate linearly (or would bit of randomness help?)
        double delayRatio =  ((double) above) / (double) (_dropThreshold - _delayThreshold);
        return _minDelayMsecs + (int) (delayRatio * (_maxDelayMsecs - _minDelayMsecs));
    }

    protected Action _handleDrop() throws InterruptedException
    {
        // First: add delay, to slow down sender
        Thread.sleep(_maxDelayMsecs);
        // and then indicate that operation was dropped
        return Action.DROP;
    }
}
