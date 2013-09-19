package com.fasterxml.clustermate.service.util;

import com.fasterxml.storemate.shared.hash.HashConstants;

public class StoreUtil
{
    public static boolean needToPullRemoteToResolve(long localTimestamp, int localContentHash,
            long remoteTimestamp, int remoteContentHash)
    {
        // If hashes match, no need to pull; ditto of remote is missing hash
        if (localContentHash == remoteContentHash || remoteContentHash == HashConstants.NO_CHECKSUM) {
            return false;
        }
        // but needs to pull if (a) remote has lower timestamp (added earlier), or,
        // (b) timestamps same, but remote has higher hash value
        if (localTimestamp < remoteTimestamp) {
            return false;
        }
        if (localTimestamp == remoteTimestamp && localContentHash > remoteContentHash) {
            return false;
        }
        return true;
    }

}
