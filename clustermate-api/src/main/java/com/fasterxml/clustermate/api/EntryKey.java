package com.fasterxml.clustermate.api;

import com.fasterxml.storemate.shared.StorableKey;

/**
 * Base class for application-specific "refined" keys, constructed usually
 * from {@link StorableKey}.
 *<p>
 * Instances must be immutable and usable as {@link java.util.Map} keys.
 */
public abstract class EntryKey implements StorableKey.Convertible
{
    @Override
    public abstract StorableKey asStorableKey();

    public abstract byte[] asBytes();
}
