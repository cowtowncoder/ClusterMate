package com.fasterxml.clustermate.client.operation;

import java.util.*;

import com.fasterxml.clustermate.api.msg.ItemInfo;
import com.fasterxml.clustermate.client.ClusterServerNode;
import com.fasterxml.clustermate.client.NodeFailure;
import com.fasterxml.clustermate.client.call.ReadCallResult;

/**
 * Result value produced by <code>EntryInspector</code>, contains information
 * about success of individual call, as well as sequence of listed
 * entries in case of successful call.
 */
public class InfoOperationResult<I extends ItemInfo>
    extends OperationResultImpl<InfoOperationResult<I>>
    implements Iterable<ReadCallResult<I>>
{
    protected List<ReadCallResult<I>> _info;

    protected int _successCount;

    protected int _missingCount;
    
    public InfoOperationResult(OperationConfig config, int approxSize)
    {
        super(config);
        _info = new ArrayList<ReadCallResult<I>>(Math.max(1, approxSize));
    }

    /*
    /**********************************************************************
    /* Building
    /**********************************************************************
     */

    public InfoOperationResult<I> withSuccess(ReadCallResult<I> callResult) {
        ++_successCount;
        _info.add(callResult);
        return this;
    }

    public InfoOperationResult<I> withFailed(ReadCallResult<I> callResult) {
        return withFailed(callResult, new NodeFailure(callResult.getServer(), callResult.getFailure()));
    }

    public InfoOperationResult<I> withFailed(ReadCallResult<I> callResult, NodeFailure fail) {
        _info.add(callResult);
        super.withFailed(fail);
        return this;
    }
    
    public InfoOperationResult<I> withMissing(ReadCallResult<I> callResult) {
        ++_missingCount;
        _info.add(callResult);
        return this;
    }
    
    @Override
    public InfoOperationResult<I> withFailed(Collection<NodeFailure> fails) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InfoOperationResult<I> withIgnored(ClusterServerNode server) {
        throw new UnsupportedOperationException();
    }
    
    /*
    /**********************************************************************
    /* Accessors, default
    /**********************************************************************
     */

    @Override
    public int getSuccessCount() {
        return _successCount;
    }

    @Override
    public OperationConfig getConfig() {
        return _config;
    }

    /*
    /**********************************************************************
    /* Accessors, additional
    /**********************************************************************
     */

    @Override
    public Iterator<ReadCallResult<I>> iterator() {
        return _info.iterator();
    }

    public ReadCallResult<I> get(int index) {
        if (index < 0 || index >= _info.size()) {
            return null;
        }
        return _info.get(index);
    }
    
    public int getMissingCount() {
        return _missingCount;
    }
}
