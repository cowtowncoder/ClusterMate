package com.fasterxml.clustermate.service.util;

import java.io.*;

public class StatsCollectingInputStream extends FilterInputStream
{
    protected long _bytesRead;
    
    public StatsCollectingInputStream(InputStream in) {
        super(in);
    }

    public long getBytesRead() {
        return _bytesRead;
    }
    
    @Override
    public int read() throws IOException {
        int c = in.read();
        if (c >= 0) {
            ++_bytesRead;
        }
        return c;
    }

    @Override
    public int read(byte[] b) throws IOException {
        int count = in.read(b);
        if (count > 0) {
            _bytesRead += count;
        }
        return count;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int count = in.read(b, off, len);
        if (count > 0) {
            _bytesRead += count;
        }
        return count;
    }

    @Override
    public long skip(long n) throws IOException {
        long count = in.skip(n);
        if (count > 0L) {
            _bytesRead += count;
        }
        return count;
    }
}
