package com.fasterxml.clustermate.servlet;

/**
 * Factory used for constructing the main dispatcher servlet used by
 * ClusterMate(-based) service.
 */
public abstract class CMServletFactory
{
    public abstract ServletBase contructDispatcherServlet();
}
