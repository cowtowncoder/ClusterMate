package com.fasterxml.clustermate.service.msg;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
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
