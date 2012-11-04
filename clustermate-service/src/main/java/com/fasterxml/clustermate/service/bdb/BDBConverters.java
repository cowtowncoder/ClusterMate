package com.fasterxml.clustermate.service.bdb;

import com.fasterxml.storemate.shared.util.WithBytesCallback;
import com.sleepycat.je.DatabaseEntry;

public class BDBConverters
{
    /**
     * Converter that converts whole key (or whatever source is)
     * into BDB-JE key.
     */
    public final static SimpleConverter simpleConverter = new SimpleConverter();
    
    private static class SimpleConverter
        implements WithBytesCallback<DatabaseEntry>
    {
        @Override
        public DatabaseEntry withBytes(byte[] buffer, int offset, int length) {
            if (offset == 0 && length == buffer.length) {
                return new DatabaseEntry(buffer);
            }
            return new DatabaseEntry(buffer, offset, length);
        }
    }

    public final static void putLongBE(byte[] buffer, int offset, long value)
    {
        putIntBE(buffer, offset, (int) (value >>> 32));
        putIntBE(buffer, offset+4, (int) value);
    }

    public final static void putIntBE(byte[] buffer, int offset, int value)
    {
        buffer[offset] = (byte) (value >> 24);
        buffer[++offset] = (byte) (value >> 16);
        buffer[++offset] = (byte) (value >> 8);
        buffer[++offset] = (byte) value;
    }

    public final static long getLongBE(byte[] buffer, int offset)
    {
        long l1 = getIntBE(buffer, offset);
        long l2 = getIntBE(buffer, offset+4);
        return (l1 << 32) | ((l2 << 32) >>> 32);
    }
    
    public final static int getIntBE(byte[] buffer, int offset)
    {
        return (buffer[offset] << 24)
            | ((buffer[++offset] & 0xFF) << 16)
            | ((buffer[++offset] & 0xFF) << 8)
            | (buffer[++offset] & 0xFF)
            ;
    }
}
