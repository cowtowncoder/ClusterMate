package com.fasterxml.clustermate.service.servlet;

import java.io.IOException;
import java.util.EnumMap;

import javax.servlet.http.HttpServletRequest;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.RequestPathStrategy;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.storemate.store.util.OperationDiagnostics;

/**
 * "Uber-servlet" that may be used to route requests to handlers
 * (sync, node status, store), instead of handler-specific servlets.
 */
@SuppressWarnings("serial")
public class ServiceDispatchServlet<
    K extends EntryKey,
    E extends StoredEntry<K>,
    P extends Enum<P>
>
    extends ServletBase
{
    protected final RequestPathStrategy<?> _pathStrategy;

    protected final EnumMap<P, ServletBase> _servletsByPath;
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    /**
     * Constructor used when the servlet has been registered at proper
     * root for resolving references to entry points, as per
     * configured
     */
    public ServiceDispatchServlet(ClusterViewByServer clusterView, String servletPathBase,
            SharedServiceStuff stuff,
            EnumMap<P,ServletBase> servlets)
    {
        // null -> use servlet path base as-is
        super(stuff, clusterView, servletPathBase);
        _pathStrategy = stuff.getPathStrategy();
        _servletsByPath = servlets;
    }

    /*
    /**********************************************************************
    /* Overrides
    /**********************************************************************
     */

    @Override
    protected ServletServiceRequest constructRequest(HttpServletRequest orig)
    {
        String path = orig.getRequestURI();

        /* First things first: remove servlet path prefix (either override,
         * or one used for registration)
         */
        if (_basePath == null) { // no base path; use whatever it was registered with
            path = _trimPath(path, orig.getServletPath(), true);
        } else {
            path = _trimPath(path, _basePath, false);
        }
        // false -> not URL decoded
        return new ServletServiceRequest(orig, path, false);
    }
    
    /*
    /**********************************************************************
    /* Main dispatch methods
    /**********************************************************************
     */

    @Override
    public void handleGet(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics metadata) throws IOException
    {
        ServletBase servlet = _matchServlet(request);
        if (servlet != null) {
            servlet.handleGet(request, response, metadata);
            return;
        }
        response = response.notFound();
        response.writeOut(null);
    }

    @Override
    public void handleHead(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics metadata) throws IOException
    {
        ServletBase servlet = _matchServlet(request);
        if (servlet != null) {
            servlet.handleHead(request, response, metadata);
            return;
        }
        response = response.notFound();
        response.writeOut(null);
    }

    @Override
    public void handlePut(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics metadata) throws IOException
    {
        ServletBase servlet = _matchServlet(request);
        if (servlet != null) {
            servlet.handlePut(request, response, metadata);
            return;
        }
        response = response.notFound();
        response.writeOut(null);
    }

    @Override
    public void handlePost(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics metadata) throws IOException
    {
        ServletBase servlet = _matchServlet(request);
        if (servlet != null) {
            servlet.handlePost(request, response, metadata);
            return;
        }
        response = response.notFound();
        response.writeOut(null);
    }

    @Override
    public void handleDelete(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics metadata) throws IOException
    {
        ServletBase servlet = _matchServlet(request);
        if (servlet != null) {
            servlet.handleDelete(request, response, metadata);
            return;
        }
        response = response.notFound();
        response.writeOut(null);
    }
    
    /*
    /**********************************************************************
    /* Helper methods
    /**********************************************************************
     */

    protected ServletBase _matchServlet(ServletServiceRequest request)
    {
        @SuppressWarnings("unchecked")
        P type = (P) _pathStrategy.matchPath(request);
        if (type != null) {
            return _servletsByPath.get(type);
        }
        return null;
    }
}
