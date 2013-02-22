package com.fasterxml.clustermate.service.store;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.hash.HashConstants;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.util.OverwriteChecker;

public class AllowUndeletingUpdates implements OverwriteChecker
{
    public final static OverwriteChecker instance = new AllowUndeletingUpdates();

    // Nope, can't say without seeing entries in question
    @Override
    public Boolean mayOverwrite(StorableKey key) { return null; }

    /**
     * Logic for overwrite are such that we may overwrite (soft) deleted entries, as 
     * long as hash codes are compatible
     */
    @Override
    public boolean mayOverwrite(StorableKey key, Storable oldEntry, Storable newEntry)
    {
        return oldEntry.isDeleted()
                && _hashCodesMatch(oldEntry.getContentHash(), newEntry.getContentHash())
                && _hashCodesMatch(oldEntry.getCompressedHash(), newEntry.getCompressedHash())
                ;
    }

    protected boolean _hashCodesMatch(int hash1, int hash2) {
        // if either one is missing, need to accept
        if (hash1 == HashConstants.CHECKSUM_FOR_ZERO || hash2 == HashConstants.CHECKSUM_FOR_ZERO) {
            return true;
        }
        return (hash1 == hash2);
    }
}
