package com.fasterxml.clustermate.client.operation;

import java.util.*;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.client.*;
import com.fasterxml.clustermate.client.call.CallFailure;
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

    protected final int _maxCallRetries;

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

    protected long _roundStartTime;

    // // // State

    /**
     * Number of round(s) of calls completed: each round consists of calling
     * a subset of available nodes.
     */
    protected int _round;

    /**
     * We need to keep a list of active nodes with possible past and future
     * calls; ones that we may keep trying to call, and that have not yet
     * been added to result object as success or fail.
     */
    protected final List<PutCallState> _activeNodes;

    protected Iterator<PutCallState> _currentNodes;
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    public PutOperationImpl(CONFIG config, long startTime,
            NodesForKey serverNodes, K key,
            PutCallParameters params, PutContentProvider content)            
    {
        super(config, startTime, key);

        // may make configurable in future but for now is static (not very useful to change)
        _maxCallRetries = StoreClientConfig.MAX_RETRIES_FOR_PUT;  

        _params = params;
        _content = content;
        _result = new PutOperationResult(_operationConfig, _params);

        final int serverCount = serverNodes.size();
        if (serverCount == 0) {
            _activeNodes = Collections.emptyList();
        } else {
            _activeNodes = new ArrayList<PutCallState>(serverCount);
            for (int i = 0; i < serverCount; ++i) {
                _activeNodes.add(new PutCallState(serverNodes.node(i)));
            }
        }

        // Then figure out how long we have for the whole operation
        _endOfTime = _startTime + _operationConfig.getGetOperationTimeoutMsecs();
        _lastValidTime = _endOfTime - _callConfig.getMinimumTimeoutMsecs();
    }

    @Override
    public PutOperationResult finish()
    {
        if (!_released) {
            _content.release();
            _released = true;
        }
        for (PutCallState state : _activeNodes) {
            NodeFailure getFails = state.getFails();
            if (getFails == null) {
                _result.withIgnored(state.server());
            } else {
                _result.withFailed(getFails);
            }
        }
        return _result;
    }

    /*
    /**********************************************************************
    /* Public API implementation
    /**********************************************************************
     */

    @Override
    public PutOperationResult result() {
        return _result;
    }

    @Override
    public int completedRounds() {
        return _round;
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
            final PutCallState call = _currentNodes.next();
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
            final PutCallState call = _currentNodes.next();
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

    /*
    /**********************************************************************
    /* Helper class(es)
    /**********************************************************************
     */

    /**
     * Container used to hold in-flight information about calls to a single applicable
     * target node.
     */
    protected final static class PutCallState
    {
        protected final ClusterServerNode _node;

        protected NodeFailure _fails;
        
        public PutCallState(ClusterServerNode node)
        {
            _node = node;
        }

        public void addFailure(CallFailure fail) {
            if (_fails == null) {
                _fails = new NodeFailure(_node, fail);
            } else {
                _fails.addFailure(fail);
            }
        }
        
        public ClusterServerNode server() { return _node; }

        public NodeFailure getFails() { return _fails; }
    }
}
