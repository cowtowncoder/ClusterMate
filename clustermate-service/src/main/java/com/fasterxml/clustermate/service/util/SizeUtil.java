package com.fasterxml.clustermate.service.util;

public class SizeUtil
{
    public static String sizeDesc(long amount)
    {
        // yeah I'm old-fashioned, kilo-byte is 2^10 bytes
        double kbs = amount / 1024.0;
        if (kbs < 1000.0) {
            return String.format("%.1f kB", kbs);
        }
        double mbs = kbs / 1024.0;
        if (mbs < 1000.0) {
            return String.format("%.1f MB", mbs);
        }
        double gigs = mbs / 1024.0;
        return String.format("%.1f GB", gigs);
    }

}
