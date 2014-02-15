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

    // instead of 'getCompression', to save space
    @JsonProperty
    protected Character comp;

    protected int hash;

    @JsonProperty
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
        comp = compression;
        this.hash = hash;
    }
    
    protected ItemInfo(ItemInfo base) {
        modtime = base.modtime;
        length = base.length;
        compressedLength = base.compressedLength;
        comp = base.comp;
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
    
    /*
    /**********************************************************************
    /* Convenience accessors
    /**********************************************************************
     */

    @JsonIgnore
    public Compression getCompression() {
        if (comp == null) {
            return Compression.NONE;
        }
        char c = comp.charValue();
        for (Compression cmp : Compression.values()) {
            if (c == cmp.asChar()) {
                return cmp;
            }
        }
        // let's allow empty for bit more robustness
        if (c == ' ' || c == '\0') {
            return Compression.NONE;
        }
        throw new IllegalStateException("Invalid compression type '"+comp+"' (0x"
                +Integer.toHexString(c)+")");
    }
    
    @JsonIgnore
    public boolean isCompressed() {
        return (comp == null) || (comp.charValue() == Compression.NONE.asChar());
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
