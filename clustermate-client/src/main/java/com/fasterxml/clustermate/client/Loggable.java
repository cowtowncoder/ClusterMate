package com.fasterxml.clustermate.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class defined just to allow overriding logging of warning
 * messages, should someone deeply care
 */
public abstract class Loggable
{
    protected final Logger _logger;

    protected Loggable() {
        _logger = LoggerFactory.getLogger(getClass());
    }
    
    protected Loggable(Class<?> loggingFor) {
        _logger = LoggerFactory.getLogger(loggingFor);
    }

    public boolean isInfoEnabled() {
        return _logger.isInfoEnabled();
    }
    
    public void logInfo(String msg) {
        _logger.info(msg);
    }

    public void logInfo(String msg, Object... args) {
        _logger.info(msg, args);
    }
    
    public void logWarn(String msg) {
        _logger.warn(msg);
    }

    public void logWarn(String msg, Object... args) {
        _logger.warn(msg, args);
    }
    
    public void logWarn(Throwable t, String msg) {
        _logger.warn(msg, t);
    }

    public void logError(String msg) {
        _logger.error(msg);
    }

    public void logError(String msg, Object... args) {
        _logger.error(msg, args);
    }
    
    public void logError(Throwable t, String msg) {
        _logger.error(msg, t);
    }
}
