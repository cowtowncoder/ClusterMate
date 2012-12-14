package com.fasterxml.clustermate.api;

import java.io.*;

import com.fasterxml.storemate.shared.IpAndPort;

/**
 * Helper class that handles details of getting cluster status information
 * from a store node.
 */
public abstract class ClusterStatusAccessor
{
    protected final static long MIN_TIMEOUT_MSECS = 10L;

    public abstract ClusterStatusMessage getClusterStatus(IpAndPort ip, long timeoutMsecs)
        throws IOException;

    public abstract ClusterStatusMessage getClusterStatus(String endpoint, long timeoutMsecs)
        throws IOException;
    
    /**
     * Simple interface for thing used to serialize payload of cluster status end point.
     */
    public abstract static class Converter {
        public abstract ClusterStatusMessage fromJSON(InputStream in) throws IOException;
        public abstract ClusterStatusMessage fromJSON(byte[] msg, int offset, int len) throws IOException;
        
        public abstract void asJSON(ClusterStatusMessage msg, OutputStream out) throws IOException;
        public abstract byte[] asJSONBytes(ClusterStatusMessage msg, OutputStream out) throws IOException;
    }
}
