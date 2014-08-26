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

    /**
     * Flag that indicates that deletion request is fully complete, if known
     * at the time of response.
     * Note that this includes both completion of synchronous deletion AND that
     * there are no more entries matching prefix (if prefix is used).
     */
    public boolean complete;

    public DeleteResponse(K key, int count, boolean complete) {
        super(key, "OK");
        this.count = count;
        this.complete = complete;
    }

    // For errors
    public DeleteResponse(K key, String message, int count) {
        super(key, message);
        this.count = count;
        this.complete = false;
    }
}
