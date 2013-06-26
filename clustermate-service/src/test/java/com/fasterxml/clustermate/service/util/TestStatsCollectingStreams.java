package com.fasterxml.clustermate.service.util;

import java.io.*;

import junit.framework.TestCase;

public class TestStatsCollectingStreams extends TestCase
{
    public void testInputStream() throws Exception
    {
        ByteArrayInputStream bytes = new ByteArrayInputStream(new byte[111]);
        StatsCollectingInputStream stats = new StatsCollectingInputStream(bytes);
        byte[] buf = new byte[40];

        while ((stats.read(buf)) > 0) { }

        assertEquals(111L, stats.getBytesRead());
        stats.close();
        assertEquals(111L, stats.getBytesRead());

        // and another access method
        bytes = new ByteArrayInputStream(new byte[137]);
        stats = new StatsCollectingInputStream(bytes);
        
        while ((stats.read(buf, 1, buf.length - 5)) > 0) { }

        assertEquals(137, stats.getBytesRead());
        stats.close();
        assertEquals(137, stats.getBytesRead());
    }
}
