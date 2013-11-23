package com.fasterxml.clustermate.service.util;

import org.slf4j.Logger;

/**
 * @deprecated Use {@link com.fasterxml.storemate.store.util.SimpleLogThrottler} directly.
 */
@Deprecated
public class SimpleLogThrottler
    extends com.fasterxml.storemate.store.util.SimpleLogThrottler
{
    public SimpleLogThrottler(Logger logger, int msecsToThrottle) {
        super(logger, msecsToThrottle);
    }
}
