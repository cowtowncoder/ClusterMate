package com.fasterxml.clustermate.service.servlet;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import com.fasterxml.clustermate.api.PathType;
import com.fasterxml.clustermate.api.RequestPathStrategy;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cluster.ClusterInfoHandler;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.store.StoreHandler;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.sync.SyncHandler;
import com.fasterxml.storemate.shared.EntryKey;

/**
 * "Uber-servlet" that is usually used to route requests to handlers
 * (sync, node status, store), instead of handler-specific servlets.
 *
 */
@SuppressWarnings("serial")
public class ServiceDispatchServlet<K extends EntryKey, E extends StoredEntry<K>>
    extends ServletBase
{
    protected final ClusterInfoHandler _clusterInfoHandler;
    protected final StoreHandler<K,E> _storeHandler;

    protected final RequestPathStrategy _pathStrategy;

    // Delegatees:
    protected final SyncListServlet<K,E> _syncListServlet;
    protected final SyncPullServlet<K,E> _syncPullServlet;
    
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
            SyncHandler<K,E> syncH,
            ClusterInfoHandler clusterInfoH,
            StoreHandler<K,E> storeH)
    {
        this(clusterView, null, stuff, syncH, clusterInfoH, storeH);
    }
    
    public ServiceDispatchServlet(ClusterViewByServer clusterView, String servletPathBase,
            SharedServiceStuff stuff,
            SyncHandler<K,E> syncH,
            ClusterInfoHandler clusterInfoH,
            StoreHandler<K,E> storeH)
    {
        // null -> use servlet path base as-is
        super(clusterView, servletPathBase);
        _clusterInfoHandler = clusterInfoH;
        _storeHandler = storeH;

        _pathStrategy = stuff.getPathStrategy();

        // Then construct Servlets we delegate stuff to:
        _syncListServlet = new SyncListServlet<K,E>(stuff, clusterView, syncH);
        _syncPullServlet = new SyncPullServlet<K,E>(stuff, clusterView, syncH);
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
        return new ServletServiceRequest(orig, path);
    }
    
    /*
    /**********************************************************************
    /* API: returning Node status
    /**********************************************************************
     */

    @Override
    public void handleGet(ServletServiceRequest request, ServletServiceResponse response) throws IOException
    {
        PathType path = _pathStrategy.matchPath(request);
        if (path != null) {
            switch (path) {
            case STORE_ENTRY:
//                response = _storeHandler.
                break;
            case STORE_LIST: // TODO!
                break;
            case NODE_STATUS:
                response = _clusterInfoHandler.getStatus(request, response);
                break;
            case SYNC_LIST:
                _syncListServlet.handleGet(request, response);
                return;
            case SYNC_PULL:
                _syncPullServlet.handleGet(request, response);
                return;
            }
            response = response.badMethod()
                    .setContentTypeText().setEntity("No GET available for endpoint");
        } else {
            response = response.notFound();
        }
        response.writeOut(null);
    }
}
