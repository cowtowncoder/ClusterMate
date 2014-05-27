package com.fasterxml.clustermate.client.operation;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.client.*;
import com.fasterxml.clustermate.client.call.CallFailure;
import com.fasterxml.clustermate.client.call.PutCallParameters;
import com.fasterxml.clustermate.client.call.PutContentProvider;

public class PutOperationImpl<K extends EntryKey,
    CONFIG extends StoreClientConfig<K, CONFIG>
>
    extends WriteOperationBase<K,CONFIG,PutCallParameters,PutOperationResult>
    implements PutOperation
{
    /*
    /**********************************************************************
    /* PUT-specific config
    /**********************************************************************
     */

    protected final PutContentProvider _content;

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    public PutOperationImpl(CONFIG config, long startTime,
            NodesForKey serverNodes, K key,
            PutCallParameters params, PutContentProvider content)            
    {
        super(config, startTime, serverNodes, key, params,
                new PutOperationResult(config.getOperationConfig(), params),
                StoreClientConfig.MAX_RETRIES_FOR_PUT,
                config.getOperationConfig().getPutOperationTimeoutMsecs());
        _content = content;
    }

    @Override
    protected void _release() {
        _content.release();
    }
    
    /*
    /**********************************************************************
    /* Public API implementation
    /**********************************************************************
     */

    @Override
    public String getTypeDesc() { return "DELETE"; }

    @Override
    public PutContentProvider content() {
        return _content;
    }

    @Override
    public PutOperation completeMinimally() throws InterruptedException {
        return perform(_operationConfig.getMinimalOksToSucceed());
    }

    @Override
    public PutOperation completeOptimally() throws InterruptedException {
        return perform(_operationConfig.getOptimalOks());
    }

    @Override
    public PutOperation tryCompleteMaximally() throws InterruptedException {
        /* Here we only want to proceed, if we get all done in first round
         * without issues; sort of bonus call.
         */
        if (_round == 0) {
            performSingleRound(_operationConfig.getMaxOks());
        }
        return this;
    }
    
    @Override
    public PutOperation completeMaximally() throws InterruptedException {
        return perform(_operationConfig.getMaxOks());
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
    protected PutOperation perform(int oksNeeded) throws InterruptedException
    {
        if (_released) {
            throw new IllegalStateException("Can not call 'complete' methods after content has been released");
        }
        // already done?
        if (_shouldFinish(_result, oksNeeded)) {
            return this;
        }
        while (_round < _maxCallRetries) {
            if (_round == 0) {
                if (_performPrimary(oksNeeded)) {
                    return this;
                }
            } else {
                if (_performSecondary(oksNeeded)) {
                    return this;
                }
            }
            ++_round;
            if (_noRetries) {
                break;
            }
            
        }
        return this;
    }

    protected PutOperation performSingleRound(int oksNeeded) throws InterruptedException
    {
        if (_released) {
            throw new IllegalStateException("Can not call 'complete' methods after content has been released");
        }
        if (!_shouldFinish(_result, oksNeeded)) {
            if (_round == 0) {
                if (_performPrimary(oksNeeded)) {
                    return this;
                }
            } else {
                if (_performSecondary(oksNeeded)) {
                    return this;
                }
            }
            ++_round;
        }
        return this;
    }
    
    /**
     * @return True if processing is now complete; false if more work needed
     */
    protected boolean _performPrimary(int oksNeeded) throws InterruptedException
    {
        if (_currentNodes == null) { // for very first call
            _currentNodes = _activeNodes.iterator();
            _roundStartTime = System.currentTimeMillis();
        }
        while (_currentNodes.hasNext()) {
            final boolean includeDisabled = !_noRetries; // only try disabled ones if no retries allowed
            final SingleCallState call = _currentNodes.next();
            final ClusterServerNode server = call.server();
            if (!includeDisabled && server.isDisabled()) { // skip disabled during first round (unless no retries)
                continue;
            }
            CallFailure fail = server.entryPutter().tryPut(_callConfig, _params, _endOfTime, _key, _content);
            if (fail == null) { // success
                _currentNodes.remove();
                _result.addSucceeded(server);
                if (_shouldFinish(_result, oksNeeded)) {
                    return true;
                }
                continue;
            }
            // nope, failed. If retriable, keep; if not, add as failure, remove from active
            if (fail.isRetriable()) {
                call.addFailure(fail);
            } else {
                _currentNodes.remove();
                _result.withFailed(new NodeFailure(server, fail));
            }
        }
        return false;
    }

    protected boolean _performSecondary(int oksNeeded) throws InterruptedException
    {
        // Starting a new round? Will need bit of delay most likely
        if (_currentNodes == null) {
            if (_activeNodes.isEmpty()) { // no more nodes? We are done
                return true;
            }
            _currentNodes = _activeNodes.iterator();
            
            // If we got this far, let's accept sub-optimal outcomes as well; or, if we timed out
            final long nextRoundStart = System.currentTimeMillis();
            _doDelay(_roundStartTime, nextRoundStart, _endOfTime);
            _roundStartTime = nextRoundStart;
        }
        while (_currentNodes.hasNext()) {
            final SingleCallState call = _currentNodes.next();
            final ClusterServerNode server = call.server();
            CallFailure fail = server.entryPutter().tryPut(_callConfig, _params, _endOfTime, _key, _content);
            if (fail == null) { // success
                _currentNodes.remove();
                _result.addSucceeded(server);
                if (_shouldFinish(_result, oksNeeded)) {
                    return true;
                }
                continue;
            }
            if (fail.isRetriable()) {
                call.addFailure(fail);
            } else {
                _currentNodes.remove();
                _result.withFailed(new NodeFailure(server, fail));
            }
        }
        return false; // still node done
    }
}
