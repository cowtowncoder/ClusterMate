package com.fasterxml.clustermate.service.servlet;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.PathType;
import com.fasterxml.clustermate.api.RequestPathStrategy;
import com.fasterxml.clustermate.service.OperationDiagnostics;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.store.StoredEntry;

/**
 * "Uber-servlet" that is usually used to route requests to handlers
 * (sync, node status, store), instead of handler-specific servlets.
 *
 */
@SuppressWarnings("serial")
public class ServiceDispatchServlet<K extends EntryKey, E extends StoredEntry<K>>
    extends ServletBase
{
    protected final RequestPathStrategy _pathStrategy;

    // Delegatees:

    protected final ServletBase _nodeStatusServlet;
    protected final ServletBase _nodeMetricsServlet;

    protected final ServletBase _storeEntryServlet;

    protected final ServletBase _storeListServlet;

    protected final ServletBase _syncListServlet;
    protected final ServletBase _syncPullServlet;

    protected final ServletBase _storeFindEntryServlet;
    protected final ServletBase _storeFindListServlet;
    
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
    public ServiceDispatchServlet(ClusterViewByServer clusterView,
            SharedServiceStuff stuff,
            ServletBase nodeStatusServlet, ServletBase nodeMetricsServlet,
            ServletBase storeEntryServlet, ServletBase storeListServlet,
            ServletBase syncListServlet, ServletBase syncPullServlet)
    {
        this(clusterView, null, stuff,
                nodeStatusServlet, nodeMetricsServlet,
                syncListServlet, syncPullServlet,
                storeEntryServlet, storeListServlet);
    }
    
    public ServiceDispatchServlet(ClusterViewByServer clusterView, String servletPathBase,
            SharedServiceStuff stuff,
            ServletBase nodeStatusServlet, ServletBase nodeMetricsServlet,
            ServletBase syncListServlet, ServletBase syncPullServlet,
            ServletBase storeEntryServlet, ServletBase storeListServlet)
    {
        // null -> use servlet path base as-is
        super(clusterView, servletPathBase);

        _pathStrategy = stuff.getPathStrategy();

        _nodeStatusServlet = nodeStatusServlet;
        _nodeMetricsServlet = nodeMetricsServlet;

        _syncListServlet = syncListServlet;
        _syncPullServlet = syncPullServlet;

        _storeEntryServlet = storeEntryServlet;
        _storeListServlet = storeListServlet;

        // !!! TODO:
        _storeFindEntryServlet = null;
        _storeFindListServlet = null;
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
        PathType type = _pathStrategy.matchPath(request);
        if (type != null) {
            switch (type) {
            case NODE_STATUS:
                return _nodeStatusServlet;
            case NODE_METRICS:
                return _nodeMetricsServlet;

            case STORE_ENTRY:
                return _storeEntryServlet;
            case STORE_LIST:
                return _storeListServlet;
            case STORE_FIND_ENTRY:
                return _storeFindEntryServlet;
            case STORE_FIND_LIST:
                return _storeFindListServlet;
            case STORE_STATUS: // is this needed?
                return null;

            case SYNC_LIST:
                return _syncListServlet;
            case SYNC_PULL:
                return _syncPullServlet;
            }
        }
        return null;
    }
}
