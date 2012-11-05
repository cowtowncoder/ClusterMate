package com.fasterxml.clustermate.service.sync;

import java.util.*;

import com.fasterxml.jackson.annotation.JsonInclude;

import com.fasterxml.clustermate.service.store.StoredEntry;

/**
 * Simple response POJO used by sync end point; used both for failures
 * (for which {@link #message} is non-null) and successes (for which
 * {@link #entries} is non-null.
 *<p>
 * NOTE: only used for writing; on reading side we will use a "raw"
 * approach.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SyncListResponse<E extends StoredEntry<?>>
{
    /**
     * Error message, if any
     */
    public String message;

    /**
     * Highest timestamp observed during processing; may be higher than highest
     * timestamp of entries returned to reflect skipped entries (or in some
     * cases known "empty" time ranges)
     */
    public Long lastSeenTimestamp;
    
    public List<SyncListResponseEntry> entries;

    public SyncListResponse() { }
    public SyncListResponse(String error) { message = error; }
    public SyncListResponse(List<E> rawEntries, long lastSeen)
    {
        entries = new ArrayList<SyncListResponseEntry>(rawEntries.size());
        for (StoredEntry<?> e : rawEntries) {
            entries.add(SyncListResponseEntry.valueOf(e));
        }
        lastSeenTimestamp = lastSeen;
    }

    public int size() {
        return (entries == null) ? 0 : entries.size();
    }

    public long lastSeen() {
        return (lastSeenTimestamp == null) ? 0L : lastSeenTimestamp.longValue();
    }
}
