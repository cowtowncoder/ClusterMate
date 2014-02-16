package com.fasterxml.clustermate.client.call;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.msg.ItemInfo;
import com.fasterxml.clustermate.client.util.ContentConverter;

/**
 * Accessor used for accessing metadata for specified entry.
 */
public interface EntryInspector<K extends EntryKey>
{
    public <T extends ItemInfo> ReadCallResult<T> tryInspect(CallConfig config, long endOfTime,
            K key, ContentConverter<T> converter);
}
