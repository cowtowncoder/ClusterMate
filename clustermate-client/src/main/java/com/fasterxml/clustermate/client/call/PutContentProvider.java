package com.fasterxml.clustermate.client.call;

import java.io.File;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.compress.Compression;

/**
 * Interface that defines how calling application needs to expose data to upload,
 * so that client can upload it to multiple stores (or for possible
 * retries) as necessary.
 *<p>
 * Methods are typically called in order of:
 *<ol> 
 * <li>{@link #contentAsBytes}</li>
 * <li>{@link #contentAsFile}</li>
 * <li>{@link #contentAsStream}</li>
 *</ol> 
 * until non-null response is received; not that most implementations return null
 * from one or more of methods, to indicate that they are not optimal accessors.
 */
public interface PutContentProvider
{
    /**
     * Method that is to be called after caller is done using content
     * that is being provided. This means that no data may be accessed,
     * and provider is free to release any resources it has retained
     * (temporary files, memory buffers).
     */
    public void release();
    
    /**
     * Physical length of content; compressed content if
     * {@link #getExistingCompression()} returns actual compression
     * method.
     * 
     * @return Length of content, if known; -1 if not known
     */
    public long length();

    /**
     * Accessor used for figuring out original length of content before
     * compression; used only if compression method is used.
     * 
     * @return Length of content before compression was applied
     */
    public long uncompressedLength();

    public ByteContainer contentAsBytes();

    public File contentAsFile() throws java.io.IOException;
    
    public java.io.InputStream contentAsStream() throws java.io.IOException;

    public int getContentHash();

    public void setContentHash(int hash);

    /**
     * Accessor used to find out if content has already been compressed
     * (and is not to be compressed but should be identified as having
     * that compression); or is explicitly indicated not to be compressed
     * (value of {@link Compression#NONE}); or, that nothing is known about
     * possible existing compression (<code>null</code>).
     */
    public Compression getExistingCompression();

    /**
     * Advanced accessor that can be used to expose underlying content source;
     * but iff caller really knows what it is doing, as return type is
     * opaque.
     */
    public Object rawSource();
}
