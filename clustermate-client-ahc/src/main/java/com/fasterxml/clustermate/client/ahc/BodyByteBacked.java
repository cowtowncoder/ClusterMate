package com.fasterxml.clustermate.client.ahc;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * {@link com.ning.http.client.Body} implementation used for PUTing
 * (or POSTing) content from an in-memory byte array.
 */
public class BodyByteBacked extends BodyBase
{
    protected final byte[] _data;
    protected final int _length, _end;

    protected int _ptr;

    public BodyByteBacked(byte[] data, int offset, int len)
    {
        super();
        _data = data;
        _ptr = offset;
        _length = len;

        _end = offset + len;
    }

    @Override
    public void close() { }

    @Override
    public long getContentLength() {
        return _length;
    }

    @Override
    public long read(ByteBuffer bb) throws IOException
    {
        int left = _end - _ptr;
        if (left <= 0) {
            return -1L;
        }
        int spaceLeft = spaceLeft(bb);
        int amount = Math.min(left, spaceLeft);
        int currPtr = _ptr;

        if (amount > 0) {
            _ptr += amount;
        }
        bb.put(_data, currPtr, amount);
        return amount;
    }
}