package com.fasterxml.clustermate.service.msg;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import com.fasterxml.storemate.shared.EntryKey;

/**
 * Base class used by various response messages (mostly errors,
 * but not just them); used so caller can try to extract
 * more specific error information, beyond HTTP status code.
 */
@JsonIgnoreProperties(ignoreUnknown=true)
@JsonPropertyOrder({ "message", "key" })
public class ResponseBase<K extends EntryKey>
{
    public String key;

    public String message;

    protected ResponseBase() { }
    protected ResponseBase(K key) {
        this(key, null);
    }
    protected ResponseBase(K key, String message) {
        this.key = (key == null) ? "" : key.toString();
        this.message = message;
    }
}
