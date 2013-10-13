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
     * @return Length of content, if known; -1 if not known
     */
    public long length();
    
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
}
