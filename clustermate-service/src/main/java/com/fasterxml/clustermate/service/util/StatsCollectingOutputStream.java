package com.fasterxml.clustermate.service.util;

import java.io.*;

import com.fasterxml.clustermate.service.OperationDiagnostics;

/**
 * Helper class used to keep track of number of bytes written to outputstream.
 */
public final class StatsCollectingOutputStream extends FilterOutputStream
{
    protected final OperationDiagnostics _metadata;
    
    public StatsCollectingOutputStream(OutputStream out, OperationDiagnostics metadata) {
        super(out);
        _metadata = metadata;
    }
    
    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len)  throws IOException {
        _metadata.addBytesTransferred(len);
        out.write(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
        _metadata.addBytesTransferred(1);
        out.write(b);
    }
}