package com.fasterxml.clustermate.client.call;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.storemate.shared.ByteRange;

/**
 * Interface for a general purpose GET accessor, for a resource stored
 * in a single server; one accessor per server and end point.
 */
public interface ContentGetter<K extends EntryKey>
{
    public <T> ReadCallResult<T> tryGet(CallConfig config, ReadCallParameters params,
            long endOfTime,
            K contentId, GetContentProcessor<T> processor, ByteRange range);
}
