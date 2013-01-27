package com.fasterxml.clustermate.api.msg;

import java.util.List;

import com.fasterxml.clustermate.api.ListItemType;
import com.fasterxml.storemate.shared.StorableKey;

/**
 * Response message type for List requests.
 * 
 * @param <T> Type of entries; either simple id ({@link StorableKey}),
 *    textual name ({@link java.lang.String}) or full {@link ListItem}.
 */
public abstract class ListResponse<T> // not a CRUD request/response
{
    /**
     * Error message for failed requests
     */
    public String message;

    /**
     * Fetched items for successful requests
     */
    public List<T> items;

    public ListResponse() { }
    public ListResponse(String msg) { message = msg; }
    public ListResponse(List<T> i) { items = i; }

    public abstract ListItemType type();
    
    public static final class IdListResponse extends ListResponse<StorableKey> {
        public IdListResponse() { }
        public IdListResponse(List<StorableKey> ids) { super(ids); }
        @Override public ListItemType type() { return ListItemType.ids; }
    }

    public static final class NameListResponse extends ListResponse<String> {
        public NameListResponse() { }
        public NameListResponse(List<String> names) { super(names); }
        @Override public ListItemType type() { return ListItemType.names; }
    }

    public static final class ItemListResponse extends ListResponse<ListItem> {
        public ItemListResponse() { }
        public ItemListResponse(List<ListItem> entries) { super(entries); }
        @Override public ListItemType type() { return ListItemType.entries; }
    }
}
