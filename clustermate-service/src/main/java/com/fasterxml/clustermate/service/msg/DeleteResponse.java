package com.fasterxml.clustermate.service.msg;

import com.fasterxml.clustermate.api.EntryKey;

/**
 * POJO returned as a result of successful DELETE request
 * (which ideally should be any and all DELETE requests)
 */
public class DeleteResponse<K extends EntryKey> extends CRUDResponseBase<K>
{
    public final static String PATH_FOR_INLINED = "NA";

    /**
     * Number of entries confirmed deleted, if known; -1 if not yet known
     * (deferred deletes)
     */
    public int count;

    @Deprecated
    public DeleteResponse(K key) {
        super(key, "OK");
        count = 0;
    }

    public DeleteResponse(K key, int count) {
        super(key, "OK");
        this.count = count;
    }
}
