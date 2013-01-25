package com.fasterxml.clustermate.client.call;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.ListType;

public interface EntryLister<K extends EntryKey>
{
    public <T> ListCallResult<T> tryList(CallConfig config, long endOfTime,
            K prefix, ListType type, int maxResults,
            ContentConverter<T> converter);
}
