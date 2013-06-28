package com.fasterxml.clustermate.service.util;

import java.io.*;

import com.fasterxml.util.membuf.StreamyBytesMemBuffer;

/**
 * Helper class that can be used to expose current contents of
 * a {@link StreamyBytesMemBuffer} as {@link InputStream}.
 * Assumption is that only content that exists should be exposed;
 * and that once it is consumed, we'll consider stream to be closed:
 * this is an alternative to blocking.
 */
public class BufferBackedInputStream extends InputStream
{
    protected final StreamyBytesMemBuffer _buffer;

    public BufferBackedInputStream(StreamyBytesMemBuffer buffer)
    {
        _buffer = buffer;
    }

    @Override
    public int available() {
        long l = _buffer.available();
        if (l > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) l;
    }

    @Override
    public void close() {
        _buffer.close();
    }

    @Override
    public void mark(int readlimit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean markSupported() { return false; }

    private byte[] _tmp;
    
    @Override
    public int read() {
        if (_tmp == null) {
            _tmp = new byte[1];
        }
        int count = read(_tmp, 0, 1);
        if (count <= 0) {
            return -1;
        }
        return _tmp[0];
    }

    @Override
    public final int read(byte[] b) {
        return read(b, 0, b.length);
    }
    
    @Override
    public int read(byte[] buffer, int offset, int length) {
        int count = _buffer.readIfAvailable(buffer, offset, length);
        // need to translate 0 to -1, to signify end-of-content
        return (count <= 0) ? -1 : count;
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long skip(long n)
    {
        if (n <= 0L) {
            return n;
        }
        int max = (n < Integer.MAX_VALUE) ? (int) n : Integer.MAX_VALUE;
        return _buffer.skip(max);
    }
}
