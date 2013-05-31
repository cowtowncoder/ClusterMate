package com.fasterxml.clustermate.service.msg;

import com.fasterxml.clustermate.api.EntryKey;

/**
 * POJO returned as a result of successful DELETE request
 * (which ideally should be any and all DELETE requests)
 */
public class DeleteResponse<K extends EntryKey> extends CRUDResponseBase<K>
{
    public final static String PATH_FOR_INLINED = "NA";

    @Deprecated
    public long creationTime;

    public DeleteResponse(K key) {
        super(key, "OK");
    }

    @Deprecated
    public DeleteResponse(K key, long creationTime)
    {
        super(key, "OK");
        this.creationTime = creationTime;
    }
}
