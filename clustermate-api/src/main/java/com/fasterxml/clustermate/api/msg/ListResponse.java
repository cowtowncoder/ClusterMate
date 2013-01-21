package com.fasterxml.clustermate.api.msg;

import java.util.List;

/**
 * Response message type for List requests.
 * 
 * @param <T> Type of entries; either simple id ({@link StorableKey}) or full {@link ListItem}
 */
public class ListResponse<T> // not a CRUD request/response
{
    /**
     * Error message for failed requests
     */
    public String message;

    /**
     * Fetched items for successful requests
     */
    public List<T> items;

    public ListResponse(String msg) { message = msg; }
    public ListResponse(List<T> i) { items = i; }
}
