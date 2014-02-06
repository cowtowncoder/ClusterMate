package com.fasterxml.clustermate.servlet;

import java.util.EnumMap;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.PathType;
import com.fasterxml.clustermate.api.msg.ListItem;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.cluster.ClusterInfoHandler;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServerUpdatable;
import com.fasterxml.clustermate.service.metrics.AllOperationMetrics;
import com.fasterxml.clustermate.service.metrics.BackgroundMetricsAccessor;
import com.fasterxml.clustermate.service.store.StoreHandler;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoresImpl;
import com.fasterxml.clustermate.service.sync.SyncHandler;

public class DefaultCMServletFactory<
    K extends EntryKey,
    E extends StoredEntry<K>,
    L extends ListItem,
    SCONFIG extends ServiceConfig
>
    extends CMServletFactory
{
    /*
    /**********************************************************************
    /* Basic config
    /**********************************************************************
     */

    protected final SCONFIG _config;
    
    protected final SharedServiceStuff _serviceStuff;

    /*
    /**********************************************************************
    /* External state
    /**********************************************************************
     */
    
    protected final StoresImpl<K,E> _stores;

    /**
     * And we better hang on to cluster view as well
     */
    protected final ClusterViewByServerUpdatable _cluster;

    /*
    /**********************************************************************
    /* Service handlers
    /**********************************************************************
     */

    protected final ClusterInfoHandler _clusterInfoHandler;

    protected final SyncHandler<K,E> _syncHandler;
    
    protected final StoreHandler<K,E,L> _storeHandler;

    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */
    
    public DefaultCMServletFactory(SharedServiceStuff stuff,
            StoresImpl<K,E> stores, ClusterViewByServerUpdatable cluster,
            ClusterInfoHandler clusterInfoHandler,
            SyncHandler<K,E> syncHandler,
            StoreHandler<K,E,L> storeHandler)
    {
        _serviceStuff = stuff;
        _config = stuff.getServiceConfig();

        _stores = stores;
        _cluster = cluster;

        _clusterInfoHandler = clusterInfoHandler;
        _syncHandler = syncHandler;
        _storeHandler = storeHandler;
    }

    /*
    /**********************************************************************
    /* Public API impl
    /**********************************************************************
     */
    
    @Override
    public ServletBase contructDispatcherServlet()
    {
        EnumMap<PathType, ServletBase> servlets = new EnumMap<PathType, ServletBase>(PathType.class);
        servlets.put(PathType.NODE_STATUS, constructNodeStatusServlet());

        ServletWithMetricsBase syncListServlet = constructSyncListServlet();
        servlets.put(PathType.SYNC_LIST, syncListServlet);
        ServletWithMetricsBase syncPullServlet = constructSyncPullServlet();
        servlets.put(PathType.SYNC_PULL, syncPullServlet);
        ServletWithMetricsBase storeEntryServlet = constructStoreEntryServlet();
        servlets.put(PathType.STORE_ENTRY, storeEntryServlet);
        ServletWithMetricsBase storeListServlet = constructStoreListServlet();
        servlets.put(PathType.STORE_ENTRIES, storeListServlet);

        final BackgroundMetricsAccessor metrics = constructMetricsAccessor(
                new AllOperationMetrics.Provider[] {
                    storeEntryServlet, storeListServlet, syncListServlet, syncPullServlet
                });
        servlets.put(PathType.NODE_METRICS, constructNodeMetricsServlet(metrics));
        return new ServiceDispatchServlet<K,E,PathType>(_cluster, null, _serviceStuff, servlets);
    }

    /*
    /**********************************************************************
    /* Factory methods: metrics
    /**********************************************************************
     */
    
    protected BackgroundMetricsAccessor constructMetricsAccessor(AllOperationMetrics.Provider[] metrics) {
        return new BackgroundMetricsAccessor(_serviceStuff, _stores, metrics);
    }
    
    /*
    /**********************************************************************
    /* Factory methods: servlets
    /**********************************************************************
     */

    protected ServletWithMetricsBase constructStoreEntryServlet() {
        return new StoreEntryServlet<K,E>(_serviceStuff, _cluster, _storeHandler);
    }

    protected ServletBase constructNodeStatusServlet() {
        return new NodeStatusServlet(_serviceStuff, _clusterInfoHandler);
    }

    protected ServletBase constructNodeMetricsServlet(BackgroundMetricsAccessor accessor) {
        return new NodeMetricsServlet(_serviceStuff, accessor);
    }
        
    protected ServletWithMetricsBase constructSyncListServlet() {
        return new SyncListServlet<K,E>(_serviceStuff, _cluster, _syncHandler);
    }

    protected ServletWithMetricsBase constructSyncPullServlet() {
        return new SyncPullServlet<K,E>(_serviceStuff, _cluster, _syncHandler);
    }

    protected ServletWithMetricsBase constructStoreListServlet() {
        return new StoreListServlet<K,E>(_serviceStuff, _cluster, _storeHandler);
    }
}
