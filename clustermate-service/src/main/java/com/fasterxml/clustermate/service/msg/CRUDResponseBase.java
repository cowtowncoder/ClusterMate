package com.fasterxml.clustermate.service.msg;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;


/**
 * Base class used by various response messages (mostly errors,
 * but not just them) used with main CRD operations (single-entry
 * create/retrive/delete) used so caller can try to extract
 * more specific error information, beyond HTTP status code.
 */
@JsonIgnoreProperties(ignoreUnknown=true)
@JsonPropertyOrder({ "message", "key" })
public class CRUDResponseBase<K extends EntryKey>
{
    public String key;

    public String message;

    protected CRUDResponseBase() { }
    protected CRUDResponseBase(K key) {
        this(key, null);
    }
    protected CRUDResponseBase(K key, String message) {
        this.key = (key == null) ? "" : key.toString();
        this.message = message;
    }
}
