package com.fasterxml.clustermate.client.call;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.ListItemType;
import com.fasterxml.clustermate.api.msg.ListResponse;
import com.fasterxml.clustermate.client.util.ContentConverter;

/**
 * Accessor used for listing metadata for entries with specified key prefix.
 */
public interface EntryLister<K extends EntryKey>
{
    public <T> ListCallResult<T> tryList(CallConfig config, long endOfTime,
            K prefix, K lastSeen, ListItemType type, int maxResults,
            ContentConverter<ListResponse<T>> converter);
}
