package com.fasterxml.clustermate.client.operation;

import java.util.*;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.client.StoreClientConfig;
import com.fasterxml.clustermate.client.call.CallConfig;

public class OperationBase<K extends EntryKey,
    CONFIG extends StoreClientConfig<K, CONFIG>
>
{
    protected final CONFIG _config;
    protected final OperationConfig _operationConfig;
    protected final CallConfig _callConfig;

    protected final boolean _noRetries;
    
    protected final long _startTime;
    protected final K _key;

    public OperationBase(CONFIG config, long startTime, K key)
    {
        _config = config;
        _operationConfig = config.getOperationConfig();
        _callConfig = config.getCallConfig();
        _noRetries = !config.getOperationConfig().getAllowRetries();
        _startTime = startTime;
        _key = key;
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
}
