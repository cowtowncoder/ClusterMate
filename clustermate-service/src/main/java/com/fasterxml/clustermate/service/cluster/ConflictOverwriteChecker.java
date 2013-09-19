package com.fasterxml.clustermate.service.cluster;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StoreException;
import com.fasterxml.storemate.store.util.OverwriteChecker;

/**
 * {@link OverwriteChecker} that is used when Sync-pull tries to ovewrite an
 * existing entry. It means that either a local entry that was missing at sync-list
 * has been added; or that a conflict was found to be resolved when handling sync-list.
 * This means that we need to figure out which case it is, which determines whether
 * locally stored entry should be overwritten or not.
 */
public class ConflictOverwriteChecker implements OverwriteChecker
{
    protected final long _newTimestamp;

    public ConflictOverwriteChecker(long newTimestamp) {
        _newTimestamp = newTimestamp;
    }
    
    @Override
    public Boolean mayOverwrite(StorableKey key) {
        /* We can't decide this solely based on key: return null to
         * indicate "maybe"
         */
        return null;
    }

    @Override
    public boolean mayOverwrite(StorableKey key, Storable oldEntry,
            Storable newEntry) throws StoreException
    {
        // First: if checksums same, no point overwriting
        final int localHash = oldEntry.getContentHash();
        final int remoteHash = newEntry.getContentHash();
        
        if (localHash == remoteHash) {
            return false;
        }

        // Second: if existing timestamp exceeds "new" one, no overwriting
        final long localTimestamp = oldEntry.getLastModified();
        final long remoteTimestamp = _newTimestamp;

        if (localTimestamp < remoteTimestamp) {
            return false;
        }
        // So: overwrite ok if remote timestamp exceeds; or, timestamps same,
        // but remote content hash "bigger" (arbitrary choice, but stable)
        return (localTimestamp > remoteTimestamp) || (remoteHash > localHash);
    }
}
