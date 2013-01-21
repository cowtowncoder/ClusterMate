package com.fasterxml.clustermate.api.msg;

import com.fasterxml.storemate.shared.StorableKey;

/**
 * Full result entry contained in {@link ListResponse} (as an alternative
 * to ids, {@link StorableKey}).
 */
public class ListItem
{
    public final StorableKey key;

    public ListItem(StorableKey k) {
        key = k;
    }
}
