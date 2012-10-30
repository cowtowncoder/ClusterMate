package com.fasterxml.clustermate.service.msg;

import com.fasterxml.storemate.shared.EntryKey;

/**
 * Simple POJO used for enclosing information in case of a failed GET
 */
public class GetErrorResponse<K extends EntryKey> extends ResponseBase<K>
{
    public GetErrorResponse(K key, String message) {
        super(key, message);
    }
}
