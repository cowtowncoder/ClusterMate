package com.fasterxml.clustermate.service.msg;

import com.fasterxml.storemate.shared.EntryKey;

/**
 * POJO returned as a result of successful DELETE request
 * (which ideally should be any and all DELETE requests)
 */
public class DeleteResponse<K extends EntryKey> extends ResponseBase<K>
{
    public final static String PATH_FOR_INLINED = "NA";

    public long creationTime;
	
    public DeleteResponse(K key, long creationTime)
    {
        super(key, "OK");
        this.creationTime = creationTime;
    }
}
