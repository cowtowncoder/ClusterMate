package com.fasterxml.clustermate.service.util;

import java.io.*;

import com.fasterxml.clustermate.service.OperationDiagnostics;

public class StatsCollectingInputStream extends FilterInputStream
{
    protected final OperationDiagnostics _metadata;
    
    public StatsCollectingInputStream(InputStream in, OperationDiagnostics metadata) {
        super(in);
        _metadata = metadata;
    }

    @Override
    public int read() throws IOException {
        int c = in.read();
        if (c >= 0) {
            _metadata.addBytesTransferred(1);
        }
        return c;
    }

    @Override
    public int read(byte[] b) throws IOException {
        int count = in.read(b);
        if (count > 0) {
            _metadata.addBytesTransferred(count);
        }
        return count;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int count = in.read(b, off, len);
        if (count > 0) {
            _metadata.addBytesTransferred(count);
        }
        return count;
    }

    @Override
    public long skip(long n) throws IOException {
        long count = in.skip(n);
        if (count > 0L) {
            _metadata.addBytesTransferred(count);
        }
        return count;
    }
}
