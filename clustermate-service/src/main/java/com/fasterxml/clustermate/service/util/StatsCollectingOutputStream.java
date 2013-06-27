package com.fasterxml.clustermate.service.util;

import java.io.*;

public class StatsCollectingOutputStream extends OutputStream
{
    protected final OutputStream _out;

    protected long _bytesWritten;

    protected boolean _closed;

    public StatsCollectingOutputStream(OutputStream out) {
        _out = out;
    }

    public long getBytesWritten() {
        return _bytesWritten;
    }

    @Override
    public void close() throws IOException
    {
        _closed = true;
        _out.close();
    }

    @Override
    public void flush() throws IOException
    {
        _out.flush();
    }

    @Override
    public final void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException
    {
        _checkClosed();
        _out.write(b, off, len);
        _bytesWritten += len;
    }

    @Override
    public void write(int b) throws IOException
    {
        _checkClosed();
        _out.write(b);
        ++_bytesWritten;
    }


    private final void _checkClosed() throws IOException {
        if (_closed) {
            throw new IOException("Can not write to "+getClass().getName()+" after close() ("
                    +_bytesWritten+" bytes written)");
        }
    }
}
