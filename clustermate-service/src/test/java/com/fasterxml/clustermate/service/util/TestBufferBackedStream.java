package com.fasterxml.clustermate.service.util;

import com.fasterxml.util.membuf.*;

import junit.framework.TestCase;

public class TestBufferBackedStream extends TestCase
{
    public void testSimple() throws Exception
    {
        MemBuffersForBytes factory = new MemBuffersForBytes(1000, 2, 10);
        StreamyBytesMemBuffer buf = factory.createStreamyBuffer(2, 5);

        for (int i = 0; i < 4500; ++i) {
//            buf.append((byte) i);
            buf.append(new byte[] { (byte) i });
        }
        BufferBackedInputStream in = new BufferBackedInputStream(buf);
        assertEquals(4500, in.available());

        assertEquals(1200, in.skip(1200));

        // Older version of low-gc-membuffers has a bug in single-byte handling
        // so we need to do something different...
        byte[] b = new byte[4500];
        assertEquals(3300, buf.read(b, 1200, 3300));
        for (int i = 1200; i < 4500; ++i) {
            assertEquals(b[i] & 0xFF, i & 0xFF);
        }
        // and should be done:
        assertEquals(0, in.available());
        assertEquals(0, in.skip(100));
        assertEquals(-1, in.read());
        assertEquals(0, buf.readIfAvailable(new byte[10]));
        in.close();
    }
}
