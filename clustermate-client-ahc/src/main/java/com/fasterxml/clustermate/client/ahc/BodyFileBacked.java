package com.fasterxml.clustermate.client.ahc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation that reads content from specified {@link java.io.File}.
 *<p>
 * Chunked transfer encoding is used for files that are big enough;
 * currently cut-off point is 64k.
 */
public class BodyFileBacked extends BodyStreamBacked
{
    protected final long _length;

    public BodyFileBacked(File f, long contentLength,
            AtomicInteger checksum) throws IOException
    {
        super(new FileInputStream(f), checksum);
        _length = contentLength;
    }
    
    @Override
    public long getContentLength() {
        return _length;
    }
}