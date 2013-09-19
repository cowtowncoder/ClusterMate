package com.fasterxml.clustermate.client.ahc;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.clustermate.std.ChecksumUtil;
import com.fasterxml.storemate.shared.hash.IncrementalHasher32;
import com.fasterxml.storemate.shared.util.BufferRecycler;

/**
 * Intermediate implementation class to make creation of
 * streaming sources easier.
 */
public abstract class BodyStreamBacked extends BodyBase
{
    /**
     * We need a basic copy-through buffer when streaming content, and since
     * this is a very frequent operation let's use a recyclable buffer.
     * Note that this MUST be static to use Thread-local properly.
     */
    final protected static BufferRecycler _bufferRecycler = new BufferRecycler(16000);

    protected final InputStream _input;
    protected final AtomicInteger _checksum;
    
    protected final IncrementalHasher32 _cheksumCalculator;

    private final BufferRecycler.Holder _bufferHolder;

    private byte[] _buffer;
    
    public BodyStreamBacked(InputStream in, AtomicInteger cs)
    {
        super();
        if (cs.get() == 0) { // not yet calculated, calculate!
            _checksum = cs;
            _cheksumCalculator = ChecksumUtil.startChecksum();
        } else {
            _checksum = null;
    	        _cheksumCalculator = null;
        }
        _bufferHolder = _bufferRecycler.getHolder();
        _input = in;
    }

    @Override
    public void close() throws IOException
    {
        _input.close();
        if (_cheksumCalculator != null) {
            _checksum.set((int) _cheksumCalculator.calculateHash());
        }
        /* 19-Spe-2013, tatu: Should not need this, but I have seen odd warnings about
         *   "Trying to double-return a buffer"; so let's see if we can squash it
         *   without sync (or further investigation).
         *   ... and yes, obviously something funky is going here. I blame AHC or Netty.
         */
        byte[] b = _buffer;
        if (b != null) {
            _buffer = null;
            _bufferHolder.returnBuffer(b);
        }
    }

//    public abstract long getContentLength();

    @Override
    public long read(ByteBuffer bb) throws IOException
    {
        if (_buffer == null) {
            _buffer = _bufferHolder.borrowBuffer(); // never null
        }
        int spaceLeft = spaceLeft(bb);
        int max = Math.min(_buffer.length, spaceLeft);

        int actual = _input.read(_buffer, 0, max);
        if (_cheksumCalculator != null && actual > 0) {
            _cheksumCalculator.update(_buffer, 0, actual);
        }
//        long read = _handleChunking(bb, _buffer, 0, actual);
        if (actual <= 0) { // end-of-input
            return -1L;
        }
        bb.put(_buffer, 0, actual);
        return actual;
    }
}