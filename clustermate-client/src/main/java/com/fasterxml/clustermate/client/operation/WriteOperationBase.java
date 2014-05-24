package com.fasterxml.clustermate.client.operation;

import java.util.*;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.client.NodeFailure;
import com.fasterxml.clustermate.client.NodesForKey;
import com.fasterxml.clustermate.client.StoreClientConfig;
import com.fasterxml.clustermate.client.call.CallConfig;
import com.fasterxml.clustermate.client.call.CallParameters;

/**
 * Base class for write operations (PUT, DELETE)
 */
public abstract class WriteOperationBase<K extends EntryKey,
    CONFIG extends StoreClientConfig<K, CONFIG>,
    PARAMS extends CallParameters,
    RESULT extends WriteOperationResult<RESULT>
>
{
    /*
    /**********************************************************************
    /* Basic configuration
    /**********************************************************************
     */

    protected final CONFIG _config;
    protected final OperationConfig _operationConfig;
    protected final CallConfig _callConfig;

    protected final boolean _noRetries;

    protected final int _maxCallRetries;
    
    /*
    /**********************************************************************
    /* Information about request itself
    /**********************************************************************
     */
    
    protected final K _key;

    protected final PARAMS _params;
    
    /**
     * Result object we will be using to pass information.
     */
    protected final RESULT _result;

    /*
    /**********************************************************************
    /* Timing constraints
    /**********************************************************************
     */

    protected final long _startTime;

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
    protected final List<SingleCallState> _activeNodes;

    protected Iterator<SingleCallState> _currentNodes;

    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */
    
    public WriteOperationBase(CONFIG config, long startTime,
            NodesForKey serverNodes, K key,
            PARAMS params, RESULT result,
            int maxCallRetries, long operationTimeoutMsecs)
    {
        _config = config;
        _operationConfig = config.getOperationConfig();
        _callConfig = config.getCallConfig();
        _key = key;
        _params = params;
        _result = result;
        _noRetries = !config.getOperationConfig().getAllowRetries();
        _startTime = startTime;
        _endOfTime = startTime + operationTimeoutMsecs;
        _lastValidTime = _endOfTime - _callConfig.getMinimumTimeoutMsecs();

        // may make configurable in future but for now is static (not very useful to change)
        _maxCallRetries = maxCallRetries;
        
        final int serverCount = serverNodes.size();
        if (serverCount == 0) {
            _activeNodes = Collections.emptyList();
        } else {
            _activeNodes = new ArrayList<SingleCallState>(serverCount);
            for (int i = 0; i < serverCount; ++i) {
                _activeNodes.add(new SingleCallState(serverNodes.node(i)));
            }
        }
    }

    /*
    /**********************************************************************
    /* Abstract methods for sub-classes to implement
    /**********************************************************************
     */

    protected abstract void _release();
    
    /*

    @Override
    public PutOperationResult finish()
    {
        if (!_released) {
            _content.release();
            _released = true;
        }
        for (SingleCallState state : _activeNodes) {
            NodeFailure getFails = state.getFails();
            if (getFails == null) {
                _result.withIgnored(state.server());
            } else {
                _result.withFailed(getFails);
            }
        }
        return _result;
    }

     */
    
    /*
    /**********************************************************************
    /* Partial WriteOperation implementation
    /**********************************************************************
     */

//    @Override
    public RESULT result() {
        return _result;
    }

//    @Override
    public int completedRounds() {
        return _round;
    }

//  @Override
    public boolean hasRemainingHosts() {
        return remainingHostCount() > 0;
    }

//  @Override
    public int remainingHostCount() {
        return _activeNodes.size();
    }


//    @Override
    public RESULT finish()
    {
        if (!_released) {
            _released = true;
            _release();
        }
        for (SingleCallState state : _activeNodes) {
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
    /* Helper methods for sub-classes
    /**********************************************************************
     */

    protected <T> List<T> _add(List<T> list, T entry) {
        if (list == null) {
            list = new LinkedList<T>();
        }
        list.add(entry);
        return list;
    }
    
    protected void _doDelay(long startTime, long currTime, long endTime)
        throws InterruptedException
    {
        long timeSpent = currTime - startTime;
        // only add delay if we have had quick failures (signaling overload)
        if (timeSpent < 1000L) {
            long timeLeft = endTime - currTime;
            // also, only wait if we still have some time; and then modest amount (250 mecs)
            if (timeLeft >= (4 * StoreClientConfig.DELAY_BETWEEN_RETRY_ROUNDS_MSECS)) {
                Thread.sleep(StoreClientConfig.DELAY_BETWEEN_RETRY_ROUNDS_MSECS);
            }
        }
    }

    protected final boolean _shouldFinish(RESULT result, int oksNeeded) {
        if (result.getSuccessCount() >= oksNeeded) {
            return true;
        }
        final long currTime = System.currentTimeMillis();
        if (currTime > _lastValidTime) {
            return true;
        }
        return false;
    }

    protected final boolean _shouldFinish(RESULT result, int oksNeeded, long currTime) {
        if (result.getSuccessCount() >= oksNeeded) {
            return true;
        }
        if (currTime > _lastValidTime) {
            return true;
        }
        return false;
    }

}
