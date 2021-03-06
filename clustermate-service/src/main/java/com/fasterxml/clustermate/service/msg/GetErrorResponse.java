package com.fasterxml.clustermate.service.msg;

import com.fasterxml.clustermate.api.EntryKey;

/**
 * Simple POJO used for enclosing information in case of a failed GET
 */
public class GetErrorResponse<K extends EntryKey> extends CRUDResponseBase<K>
{
    public GetErrorResponse(K key, String message) {
        super(key, message);
    }
}
