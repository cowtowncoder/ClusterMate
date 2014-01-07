package com.fasterxml.clustermate.api;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.hash.IncrementalHasher32;

/**
 * Abstract class that defines interface to use for converting
 * "raw" {@link StorableKey} instances into higher level key
 * abstractions, constructing such keys, and calculating
 * routing hash codes.
 */
public abstract class EntryKeyConverter<K extends EntryKey>
{
    /*
    /**********************************************************************
    /* Factory/conversion methods
    /**********************************************************************
     */

    /**
     * Method called to reconstruct an {@link EntryKey} from raw bytes.
     */
    public abstract K construct(byte[] rawKey);

    /**
     * Method called to reconstruct an {@link EntryKey} from raw bytes.
     */
    public abstract K construct(byte[] rawKey, int offset, int length);
    
    /**
     * Method called to construct a "refined" key out of raw
     * {@link StorableKey}
     */
    public abstract K rawToEntryKey(StorableKey key);

    /**
     * Optional method for converting external textual key representation
     * into internal one.
     * Useful when exposing keys to AJAX interfaces, or debugging.
     * 
     * @since 0.8.7
     */
    public abstract K stringToKey(String external);

    /**
     * Optional method for converting key into external String representation
     * (one that can be converted back using {@link #stringToKey}, losslessly).
     * Useful when exposing keys to AJAX interfaces, or debugging.
     * 
     * @since 0.8.7
     */
    public abstract String keyToString(K key);

    /**
     * Optional method for converting key into external String representation
     * (one that can be converted back using {@link #stringToKey}, losslessly).
     * Useful when exposing keys to AJAX interfaces, or debugging.
     * 
     * @since 0.8.7
     */
    public abstract String rawToString(StorableKey key);
    
    /*
    /**********************************************************************
    /* Hash code calculation
    /**********************************************************************
     */
    
    /**
     * Method called to figure out raw hash code to use for routing request
     * regarding given content key.
     */
    public abstract int routingHashFor(K key);
    
    public abstract int contentHashFor(ByteContainer bytes);

    /**
     * Method that will create a <b>new</b> hasher instance for calculating
     * hash values for content that can not be handled as a single block.
     */
    public abstract IncrementalHasher32 createStreamingContentHasher();

    /*
    /**********************************************************************
    /* Path encoding/decoding
    /**********************************************************************
     */
    
    /**
     * Method for appending key information into path, using given path builder.
     */
    public abstract <P extends Enum<P>, B extends RequestPathBuilder<P, B>> B appendToPath(B pathBuilder, K key);

    /**
     * Method for extracting key information from the path, using given path builder
     * that contains relevant remainder of path (i.e. servlet and operation parts
     * have been handled).
     */
    public abstract <P extends DecodableRequestPath> K extractFromPath(P pathBuilder);

    /*
    /**********************************************************************
    /* Helper methods
    /**********************************************************************
     */
    
    /**
     * Helper method that will 'clean up' raw hash, so that it
     * is always a non-zero positive value.
     */
    protected int _truncateHash(int hash)
    {
        if (hash > 0) {
            return hash;
        }
        if (hash == 0) { // need to mask 0
            return 1;
        }
        return hash & 0x7FFFFFFF;
    }
}
