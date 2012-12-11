package com.fasterxml.clustermate.service.util;

import java.io.*;

public class StatsCollectingInputStream extends InputStream
{
    protected final InputStream _in;
    
    protected long _bytesRead;

    protected boolean _closed;
    
    public StatsCollectingInputStream(InputStream in) {
        _in = in;
    }

    public long getBytesRead() {
        return _bytesRead;
    }

    @Override
    public int available() throws IOException {
        if (_closed) {
            return 0;
        }
        return _in.available();
    }
    
    @Override
    public void close() throws IOException {
        _closed = true;
        _in.close();
    }
    
    @Override
    public boolean markSupported() { return false; }
    
    @Override
    public int read() throws IOException {
        _checkClosed();
        int c = _in.read();
        if (c >= 0) {
            ++_bytesRead;
        }
        return c;
    }

    @Override
    public int read(byte[] b) throws IOException {
        _checkClosed();
        int count = _in.read(b);
        if (count > 0) {
            _bytesRead += count;
        }
        return count;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        _checkClosed();
        int count = _in.read(b, off, len);
        if (count > 0) {
            _bytesRead += count;
        }
        return count;
    }

    @Override
    public long skip(long n) throws IOException {
        _checkClosed();
        long count = _in.skip(n);
        if (count > 0L) {
            _bytesRead += count;
        }
        return count;
    }

    private final void _checkClosed() throws IOException {
        if (_closed) {
            throw new IOException("Can not read from "+getClass().getName()+" after close() ("
                    +_bytesRead+" bytes read)");
        }
    }
}
