package com.fasterxml.clustermate.service.sync;

import java.util.*;

import com.fasterxml.jackson.annotation.JsonInclude;

import com.fasterxml.clustermate.api.msg.ClusterStatusMessage;
import com.fasterxml.clustermate.service.store.StoredEntry;

/**
 * Simple response POJO used by sync end point; used both for failures
 * (for which {@link #message} is non-null) and successes (for which
 * {@link #entries} is non-null.
 *<p>
 * NOTE: only used for writing; on reading side we will use a "raw"
 * approach.
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
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
    public long lastSeenTimestamp;

    /**
     * Hash code server calculates over cluster view information.
     * Caller may pass it to optionally skip generation and inclusion
     * of unchanged cluster information.
     */
    public long clusterHash;

    /**
     * If server returns empty result list, it may also indicate that client
     * may want to wait for specified amount of time before issuing a new request;
     * this based on its knowledge of when more data can be available at earliest.
     * Time is in milliseconds.
     */
    public long clientWait;
    
    /**
     * Optionally included cluster view.
     */
    public ClusterStatusMessage clusterStatus;
    
    public List<SyncListResponseEntry> entries;
    
    public SyncListResponse() { }
    public SyncListResponse(String error) { message = error; }
    public SyncListResponse(List<E> rawEntries) {
        entries = new ArrayList<SyncListResponseEntry>(rawEntries.size());
        for (StoredEntry<?> e : rawEntries) {
            entries.add(SyncListResponseEntry.valueOf(e));
        }
    }

    // NOTE: only to be used internally
    private SyncListResponse(boolean dummy) {
        entries = Collections.emptyList();
    }
    
    public SyncListResponse(List<E> rawEntries, long lastSeen,
            long clusterHash, ClusterStatusMessage clusterStatus)
    {
        lastSeenTimestamp = lastSeen;
        this.clusterStatus = clusterStatus;
        this.clusterHash = clusterHash;
    }

    public static <E2 extends StoredEntry<?>> SyncListResponse<E2> emptyResponse() {
        return new SyncListResponse<E2>(false);
    }
    
    public int size() {
        return (entries == null) ? 0 : entries.size();
    }

    public long lastSeen() {
        return lastSeenTimestamp;
    }

    public SyncListResponse<E> setLastSeenTimestamp(long l) {
        lastSeenTimestamp = l;
        return this;
    }

    public SyncListResponse<E> setClusterStatus(ClusterStatusMessage s) {
        clusterStatus = s;
        return this;
    }
    
    public SyncListResponse<E> setClusterHash(long h) {
        clusterHash = h;
        return this;
    }


    public SyncListResponse<E> setClientWait(long w) {
        clientWait = w;
        return this;
    }
}
