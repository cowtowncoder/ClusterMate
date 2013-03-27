package com.fasterxml.clustermate.service;

/**
 * Simple tag interface, to denote Managed things but encapsulate
 * details on interaction with the service.
 *<p>
 * Note: while inspired by <code>Managed</code> interface of DropWizard,
 * does not implement it, to keep interaction decoupled form DW to
 * work on other containers as well.
 */
public interface StartAndStoppable
{
    public void start() throws java.lang.Exception;

    /**
     * Method called before {@link #stop}, and is meant to start graceful
     * shutdown.
     * 
     * @since 0.9.3
     */
    public void prepareForStop() throws Exception;
    
    public void stop() throws java.lang.Exception;
}
