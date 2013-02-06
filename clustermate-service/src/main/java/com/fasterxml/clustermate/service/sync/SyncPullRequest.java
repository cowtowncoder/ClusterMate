package com.fasterxml.clustermate.service.sync;

import java.util.*;

import com.fasterxml.clustermate.api.msg.ExtensibleType;
import com.fasterxml.storemate.shared.StorableKey;

/**
 * Simple value class used for containing information for
 * a "sync pull" operation.
 */
public class SyncPullRequest extends ExtensibleType
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

