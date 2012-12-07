package com.fasterxml.clustermate.service.util;

import java.io.*;

/**
 * Helper class used to keep track of number of bytes written to outputstream.
 */
public final class StatsCollectingOutputStream extends FilterOutputStream
{
    protected long _bytesWritten;
    
    public StatsCollectingOutputStream(OutputStream out) {
        super(out);
    }

    public long getBytesWritten() { return _bytesWritten; }
    
    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len)  throws IOException {
        _bytesWritten += len;
        out.write(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
        ++_bytesWritten;
        out.write(b);
    }
}