package com.fasterxml.clustermate.api.msg;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.storemate.shared.compress.Compression;

/**
 * Value class that contains metadata for a single entry; typically bit
 * more than what {@link ListItem} would contain.
 *<p>
 * Note that since this extends {@link ExtensibleType}, extension by adding
 * new entries should be relatively safe, even if old(er) clients; but
 * caller would do well to check if unrecognized properties were found.
 */
public class ItemInfo extends ExtensibleType
{
    public final static char FLAG_DELETED = 'd';
    public final static char FLAG_INLINED = 'i';
    public final static char FLAG_REPLICA = 'r';
    
    protected long modtime;

    protected long length;
    protected long compressedLength;
    
    protected Character compression;

    protected int hash;

    protected String flags;
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    /**
     * Default constructor only to be used for deserialization
     */
    protected ItemInfo() { }

    public ItemInfo(long modtime, long length, long compressedLength,
            Character compression, int hash,
            String flags)
    {
        this.modtime = modtime;
        this.length = length;
        this.compressedLength = compressedLength;
        this.compression = compression;
        this.hash = hash;
    }
    
    protected ItemInfo(ItemInfo base) {
        modtime = base.modtime;
        length = base.length;
        compressedLength = base.compressedLength;
        compression = base.compression;
        hash = base.hash;
        flags = base.flags;
    }

    /*
    /**********************************************************************
    /* API for (de)ser
    /**********************************************************************
     */
    
    // Use shorter public names to save some space
    
    public int getHash() { return hash; }
    public long getLength() { return length; }

    @JsonProperty("compLength")
    public long getCompressedLength() { return compressedLength; }

    @JsonProperty("comp")
    public Character getCompression() { return compression; }

    /*
    /**********************************************************************
    /* Convenience accessors
    /**********************************************************************
     */

    @JsonIgnore
    public boolean isCompressed() {
        return (compression == null) || (compression.charValue() == Compression.NONE.asChar());
    }

    @JsonIgnore
    public boolean isDeleted() { return (flags.indexOf(FLAG_DELETED) >= 0); }

    @JsonIgnore
    public boolean isInlined() { return (flags.indexOf(FLAG_INLINED) >= 0); }

    @JsonIgnore
    public boolean isReplica() { return (flags.indexOf(FLAG_REPLICA) >= 0); }
    
    /**
     * Convenience accessor that determines actual storage length used by server;
     * for compressed entries, it'll return same as {@link #getCompressedLength()}
     * and for others same as {@link #getLength()}.
     */
    @JsonIgnore
    public long getStorageLength() {
        if (compressedLength >= 0L) {
            return compressedLength;
        }
        return length;
    }
}
