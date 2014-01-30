package com.fasterxml.clustermate.client.operation;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.client.*;
import com.fasterxml.clustermate.client.call.PutCallParameters;
import com.fasterxml.clustermate.client.call.PutContentProvider;

public class PutOperationImpl<K extends EntryKey,
        CONFIG extends StoreClientConfig<K, CONFIG>
>
    extends OperationBase<K,CONFIG>
    implements PutOperation
{
    /*
    /**********************************************************************
    /* Timing constraints
    /**********************************************************************
     */

    /**
     * Timestamp at which the whole operation will fail.
     */
    protected final long _endOfTime;

    /**
     * Last possible time when we can start a call and have some hope
     * of it not failing for timeout.
     */
    protected final long _lastValidTime;

    /*
    /**********************************************************************
    /* PUT-specific config
    /**********************************************************************
     */
    
    protected final PutCallParameters _params;
    protected final PutContentProvider _content;
    
    /**
     * Result object we will be using to pass information.
     */
    protected final PutOperationResult _result;

    /*
    /**********************************************************************
    /* State
    /**********************************************************************
     */

    protected boolean _released;
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    public PutOperationImpl(CONFIG config, long startTime,
            NodesForKey serverNodes, K key,
            PutCallParameters params, PutContentProvider content)            
    {
        super(config, serverNodes, startTime, key);

        _params = params;
        _content = content;
        _result = new PutOperationResult(_operationConfig, _params);

        // Then figure out how long we have for the whole operation
        _endOfTime = _startTime + _operationConfig.getGetOperationTimeoutMsecs();
        _lastValidTime = _endOfTime - _callConfig.getMinimumTimeoutMsecs();
    }

    @Override
    public void release()
    {
        if (!_released) {
            _content.release();
            _released = true;
        }
    }
    
    /*
    /**********************************************************************
    /* Public API implementation
    /**********************************************************************
     */

    @Override
    public PutOperationResult getCurrentState() {
        return _result;
    }
    
    @Override
    public PutOperationResult completeMinimally() throws InterruptedException {
        return perform(_result, _operationConfig.getMinimalOksToSucceed());
    }

    @Override
    public PutOperationResult completeOptimally() throws InterruptedException {
        return perform(_result, _operationConfig.getOptimalOks());
    }

    @Override
    public PutOperationResult tryCompleteMaximally() throws InterruptedException {
        return perform(_result, _operationConfig.getMaxOks());
    }
    
    @Override
    public PutOperationResult completeMaximally() throws InterruptedException {
        return perform(_result, _operationConfig.getMaxOks());
    }

    /*
    /**********************************************************************
    /* Main call handling method(s)
    /**********************************************************************
     */

    /**
     * @param result Result object to update, return
     * @param oksNeeded Number of success nodes we need, total
     */
    protected PutOperationResult perform(PutOperationResult result, int oksNeeded)
        throws InterruptedException
    {
        if (_released) {
            throw new IllegalStateException("Can not call 'complete' methods after content has been released");
        }

        // One sanity check: if not enough server nodes to talk to, can't succeed...
        int nodeCount = _serverNodes.size();
        // should this actually result in an exception?
        if (nodeCount < _operationConfig.getMinimalOksToSucceed()) {
            return result;
        }

        /* Ok: first round; try PUT into every enabled store, up to optimal number
         * of successes we expect.
         */
        List<NodeFailure> retries = null;
        for (int i = 0; i < nodeCount; ++i) {
            ClusterServerNode server = _serverNodes.node(i);
            if (server.isDisabled() && !_noRetries) { // can skip disabled, iff retries allowed
                continue;
            }
            CallFailure fail = server.entryPutter().tryPut(_callConfig, _params, _endOfTime, _key, _content);
            if (fail != null) { // only add to retry-list if something retry may help with
                if (fail.isRetriable()) {
                    retries = _add(retries, new NodeFailure(server, fail));
                } else {
                    result.withFailed(new NodeFailure(server, fail));
                }
                continue;
            }
            result.addSucceeded(server);
            // Very first round: go up to max if it's smooth sailing!
            if (result.succeededMaximally()) {
                return result.withFailed(retries);
            }
        }
        if (_noRetries) { // if we can't retry, don't:
            return result.withFailed(retries);
        }

        // If we got this far, let's accept sub-optimal outcomes as well; or, if we timed out
        final long secondRoundStart = System.currentTimeMillis();
        if (_shouldFinish(result, oksNeeded, secondRoundStart)) {
            return result.withFailed(retries);
        }
        // Do we need any delay in between?
        _doDelay(_startTime, secondRoundStart, _endOfTime);
        
        // Otherwise: go over retry list first, and if that's not enough, try disabled
        if (retries == null) {
            retries = new LinkedList<NodeFailure>();
        } else {
            Iterator<NodeFailure> it = retries.iterator();
            while (it.hasNext()) {
                NodeFailure retry = it.next();
                ClusterServerNode server = (ClusterServerNode) retry.getServer();
                CallFailure fail = server.entryPutter().tryPut(_callConfig, _params, _endOfTime, _key, _content);
                if (fail != null) {
                    retry.addFailure(fail);
                    if (!fail.isRetriable()) { // not worth retrying?
                        result.withFailed(retry);
                        it.remove();
                    }
                } else {
                    it.remove(); // remove now from retry list
                    result.addSucceeded(server);
                    if (result.succeededOptimally()) {
                        return result.withFailed(retries);
                    }
                }
            }
        }
        // if no success, add disabled nodes in the mix; but only if we don't have minimal success:
        for (int i = 0; i < nodeCount; ++i) {
            if (_shouldFinish(result, oksNeeded)) {
                return result.withFailed(retries);
            }
            ClusterServerNode server = _serverNodes.node(i);
            if (server.isDisabled()) {
                CallFailure fail = server.entryPutter().tryPut(_callConfig,
                         _params, _endOfTime, _key, _content);
                if (fail != null) {
                    if (fail.isRetriable()) {
                        retries.add(new NodeFailure(server, fail));
                    } else {
                        result.withFailed(new NodeFailure(server, fail));
                    }
                } else {
                    result.addSucceeded(server);
                }
            }
        }

        // But from now on, keep on retrying, up to... N times (start with 1, as we did first retry)
        long prevStartTime = secondRoundStart;
        for (int i = 1; (i <= StoreClientConfig.MAX_RETRIES_FOR_PUT) && !retries.isEmpty(); ++i) {
            final long currStartTime = System.currentTimeMillis();
            _doDelay(prevStartTime, currStartTime, _endOfTime);
            // and off we go again...
            Iterator<NodeFailure> it = retries.iterator();
            while (it.hasNext()) {
                if (_shouldFinish(result, oksNeeded)) {
                    return result.withFailed(retries);
                }
                NodeFailure retry = it.next();
                ClusterServerNode server = (ClusterServerNode) retry.getServer();
                CallFailure fail = server.entryPutter().tryPut(_callConfig,
                        _params, _endOfTime, _key, _content);
                if (fail != null) {
                    retry.addFailure(fail);
                    if (!fail.isRetriable()) {
                        result.withFailed(retry);
                        it.remove();
                    }
                } else {
                    result.addSucceeded(server);
                }
            }
            prevStartTime = currStartTime;
        }
        // we are all done, failed:
        return result.withFailed(retries);
    }

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    protected boolean _shouldFinish(PutOperationResult result, int oksNeeded) {
        if (result.getSuccessCount() >= oksNeeded) {
            return true;
        }
        final long currTime = System.currentTimeMillis();
        if (currTime > _lastValidTime) {
            return true;
        }
        return false;
    }

    protected boolean _shouldFinish(PutOperationResult result, int oksNeeded, long currTime) {
        if (result.getSuccessCount() >= oksNeeded) {
            return true;
        }
        if (currTime > _lastValidTime) {
            return true;
        }
        return false;
    }
}
