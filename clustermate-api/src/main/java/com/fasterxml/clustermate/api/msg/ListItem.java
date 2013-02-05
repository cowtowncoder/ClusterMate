package com.fasterxml.clustermate.api.msg;

import com.fasterxml.storemate.shared.StorableKey;

/**
 * Full result entry contained in {@link ListResponse} (as an alternative
 * to ids, {@link StorableKey}).
 */
public class ListItem
{
    // NOTE: names MUST match with accessor names
    
    protected StorableKey key;

    protected int hash;

    protected long length;
    
    protected ListItem() { }
    
    public ListItem(StorableKey k, int h, long l)
    {
        key = k;
        hash = h;
        length = l;
    }

    protected ListItem(ListItem base) {
        key = base.key;
        hash = base.hash;
        length = base.length;
    }
    
    // Use shorter public names to save some space
    
    public StorableKey getKey() { return key; }
    public int getHash() { return hash; }
    public long getLength() { return length; }
}
