package com.fasterxml.clustermate.service.msg;

import com.fasterxml.storemate.shared.EntryKey;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.store.Storable;

/**
 * Simple value class used to return information on a successful
 * PUT request
 */
public class PutResponse<K extends EntryKey> extends CRUDResponseBase<K>
{
    public Compression compression;
	
    public long size;

    public long storageSize;

    public boolean inlined;
    
    public PutResponse() { }
    public PutResponse(K key) {
        this(key, null);
    }
    public PutResponse(K key, String msg) {
        super(key, msg);
    }

    protected PutResponse(K key, Storable entry, String msg) {
        super(key, msg);
        if (entry != null) {
            compression = entry.getCompression();
            size = entry.getOriginalLength();
            if (size == 0L) { // mark it as "not available"
                size = -1L;
            }
            storageSize = entry.getStorageLength();
            inlined = !entry.hasExternalData();
        }
    }

    public static <K extends EntryKey> PutResponse<K> error(K key, Storable newEntry, String msg) {
        return new PutResponse<K>(key, newEntry, msg);
    }

    public static <K extends EntryKey> PutResponse<K> error(K key, String msg) {
        return new PutResponse<K>(key, msg);
    }
    
    public static <K extends EntryKey> PutResponse<K> badCompression(K key, String msg) {
        return new PutResponse<K>(key, msg);
    }
    
    public static <K extends EntryKey> PutResponse<K> badArg(K key, String msg) {
        return new PutResponse<K>(key, msg);
    }
    
    /**
     * Factory method for constructing a response message that indicates
     * successful addition of given payload.
     */
    public static <K extends EntryKey> PutResponse<K> ok(K key, Storable entry) {
        return new PutResponse<K>(key, entry, null);
    }
}
