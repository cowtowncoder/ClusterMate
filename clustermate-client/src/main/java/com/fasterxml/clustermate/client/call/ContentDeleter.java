package com.fasterxml.clustermate.client.call;

import com.fasterxml.clustermate.api.EntryKey;

/**
 * Interface for a general purpose DELETE accessor, for a resource stored
 * in a single server; one accessor per server and end point.
 */
public interface ContentDeleter<K extends EntryKey>
{
    public CallFailure tryDelete(CallConfig config, DeleteCallParameters params,
            long endOfTime, K key);
}
