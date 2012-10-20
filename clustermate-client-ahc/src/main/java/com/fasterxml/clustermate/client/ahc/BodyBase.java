package com.fasterxml.clustermate.client.ahc;

import java.nio.ByteBuffer;

import com.ning.http.client.Body;

/**
 * Base class for implementations of {@link Body}, abstraction needed
 * to work with non-blocking Async HTTP Client.
 *<p>
 * Note the ugly business to work Netty's seeming inability to work
 * with Chunked transfer encoding when doing PUTs and POSTs.
 */
abstract class BodyBase implements Body
{
    protected BodyBase()
    {
//        _addChunks = false;
    }

    protected int spaceLeft(ByteBuffer bb)
    {
        int amount = bb.capacity() - bb.position();
        if (amount <= 0) { // sanity check
            throw new IllegalArgumentException("Caller passed full ByteBuffer: position="
                    +bb.position()+", capacity="+bb.capacity());
        }
        // 16-May-2012, tatu: Alas, we may need to add about 10 bytes to
        //   work around Netty bug (wrt chunked mode)
        /*
        if (_addChunks) {
            amount -= 10;
            if (amount < 1) {
                throw new IllegalArgumentException("Caller passed ByteBuffer that did not have enough space: "
                        +"position="+bb.position()+", capacity="+bb.capacity());
            }
        }
        */
        return amount;
    }
    
    /**
     * Method that is needed to work around buggy Netty implementation, which
     * does NOT handle chunking as expected: rather, we have to manually
     * implement chunking here.
     * 
     * @return Number of content bytes added; which may be less than number of
     *   bytes written actually (for chunk bug work-around)
     */

    /* 15-Oct-2012, tatu: Once upon a time, a horrible kludge like this
     *   was required due to some oddity of some version of Netty. But
     *   as far as I know, this should NOT be required any more.
     */
/*
    protected final static byte BYTE_CR = '\r';
    protected final static byte BYTE_LF = '\n';
    protected final static byte BYTE_0 = '0';

    protected final static byte[] HEX_BYTES = new byte[] {
        '0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };
    
    protected final boolean _addChunks;

    // Marker we use to keep track of whether EOF marker
    // for buggy-Netty-chunks has been sent already
    protected boolean _nettyEofSent = false;
    
    
    protected long _handleChunking(ByteBuffer bb,
            byte[] buffer, int offset, int length)
    {
//System.out.println(" ("+getClass().getName()+")["+offset+"/+"+length+"]");        
        if (length <= 0) { // end-of-input
            return -1L;
        }
        bb.put(buffer, offset, length);
        return length;

        if (length < 0) { // end-of-input
            // Work-around needed? This as per AHC sources, InputStreamBodyGenerator,
            // which claims Netty has some nasty bug(s) in handling of chunked encoding.
            if (_addChunks) {
                if (!_nettyEofSent) {
                    _nettyEofSent = true;
                    // First: must send the usual empty chunk as empty marker
                    bb.put(BYTE_0);
                    bb.put(BYTE_CR);
                    bb.put(BYTE_LF);
                    // but then also one trailing CRLF sequence
                    bb.put(BYTE_CR);
                    bb.put(BYTE_LF);
                    return 0;
                }
            }
            return -1L;
        }
        if (_addChunks) { // must prepend length as hex bytes, and line-feed
            //int bytesSent =
            _appendHex(bb, length);
            bb.put(buffer, 0, length);
            // plus, trailing CRLF as well
            bb.put(BYTE_CR);
            bb.put(BYTE_LF);
            return length;
        }
        bb.put(buffer, offset, length);
        return length;
    }

    protected int _appendHex(ByteBuffer bb, int length)
    {
        int bytes;
        if (length <= 0xF) {
            bb.put(HEX_BYTES[length]);
            bytes = 3;
        }
        else if (length <= 0xFF) {
            bb.put(HEX_BYTES[length >> 4]);
            bb.put(HEX_BYTES[length & 0xF]);
            bytes = 4;
        }
        else if (length <= 0xFFF) {
            bb.put(HEX_BYTES[length >> 8]);
            bb.put(HEX_BYTES[(length >> 4) & 0xF]);
            bb.put(HEX_BYTES[length & 0xF]);
            bytes = 5;
        }
        else {
            bb.put(HEX_BYTES[(length >> 12) & 0xF]);
            bb.put(HEX_BYTES[(length >> 8) & 0xF]);
            bb.put(HEX_BYTES[(length >> 4) & 0xF]);
            bb.put(HEX_BYTES[length & 0xF]);
            bytes = 6;
        }
        bb.put(BYTE_CR);
        bb.put(BYTE_LF);
        return bytes;
    }
        */
}
