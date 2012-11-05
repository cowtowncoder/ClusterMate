package com.fasterxml.clustermate.service.sync;

import java.util.*;

import com.fasterxml.storemate.shared.StorableKey;

/**
 * Simple value class used for containing information for
 * a "sync pull" operation (see {@link com.force.vagabond.server.jaxrs.SyncResource}
 * for details).
 */
public class SyncPullRequest
{
    public List<StorableKey> entries;
    
    public SyncPullRequest() { }

    public void addEntry(StorableKey key) {
        if (entries == null) {
            entries = new ArrayList<StorableKey>();
        }
        entries.add(key);
    }

    public int size() {
        return (entries == null) ? 0 : entries.size();
    }
}

