package com.fasterxml.clustermate.dw;

import java.io.*;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.assets.AssetsBundle;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.lifecycle.Managed;
import com.fasterxml.storemate.shared.*;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.StoreOperationThrottler;
import com.fasterxml.storemate.store.backend.StoreBackend;
import com.fasterxml.storemate.store.backend.StoreBackendBuilder;
import com.fasterxml.storemate.store.backend.StoreBackendConfig;
import com.fasterxml.storemate.store.file.FileManager;
import com.fasterxml.storemate.store.impl.StorableStoreImpl;
import com.fasterxml.storemate.store.state.NodeStateStore;
import com.fasterxml.storemate.store.util.PartitionedWriteMutex;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.PathType;
import com.fasterxml.clustermate.api.RequestPathBuilder;
import com.fasterxml.clustermate.api.msg.ListItem;
import com.fasterxml.clustermate.jaxrs.IndexResource;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.Stores;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.cleanup.CleanerUpper;
import com.fasterxml.clustermate.service.cleanup.CleanupTask;
import com.fasterxml.clustermate.service.cluster.*;
import com.fasterxml.clustermate.service.metrics.AllOperationMetrics;
import com.fasterxml.clustermate.service.metrics.BackgroundMetricsAccessor;
import com.fasterxml.clustermate.service.state.ActiveNodeState;
import com.fasterxml.clustermate.service.state.JacksonBasedConverter;
import com.fasterxml.clustermate.service.store.*;
import com.fasterxml.clustermate.service.sync.SyncHandler;
import com.fasterxml.clustermate.servlet.*;
import com.fasterxml.clustermate.std.JdkHttpClientPathBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class DWBasedService<
    K extends EntryKey,
    E extends StoredEntry<K>,
    L extends ListItem,
    SCONFIG extends ServiceConfig,
    CONF extends DWConfigBase<SCONFIG, CONF>
>
    extends Service<CONF>
{
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    /**
     * List of {@link StartAndStoppable} objects we will dispatch start/stop calls to.
     */
    protected List<StartAndStoppable> _managed = null;
    
    /**
     * Running mode of this service; used to indicate whether we are running in
     * test mode, and whether background tasks ought to be run or not.
     */
    protected final RunMode _runMode;

    /**
     * We need to keep track of things created, to let tests access
     * the information
     */
    protected SharedServiceStuff _serviceStuff;
    
    /**
     * Container for various stores we use for data, metadata.
     */
    protected StoresImpl<K,E> _stores;
    
    /**
     * This object is needed to allow test code to work around usual
     * waiting time restrictions.
     */
    protected final TimeMaster _timeMaster;

    /**
     * And we better hang on to cluster view as well
     */
    protected ClusterViewByServerUpdatable _cluster;
    
    /**
     * Manager object that deals with data expiration and related
     * clean up tasks.
     */
    protected CleanerUpper<K,E> _cleanerUpper;

    /*
    /**********************************************************************
    /* Handlers
    /**********************************************************************
     */
    
    protected StoreHandler<K,E,L> _storeHandler;
    
    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    protected DWBasedService(TimeMaster timings, RunMode mode)
    {
        super();
        _timeMaster = timings;
        _runMode = mode;
    }

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    @Override
    public void initialize(Bootstrap<CONF> bootstrap) {
        // Static stuff from under /html (except for root  level things
        // like /index.html that need special handling)
        bootstrap.addBundle(new AssetsBundle("/html"));
    }

    public void _start() throws Exception
    {
        LOG.info("Starting up {} VManaged objects", _managed.size());
        for (StartAndStoppable managed : _managed) {
            LOG.info("Starting up: {}", managed.getClass().getName());
            managed.start();
        }
        LOG.info("VManaged object startup complete");

        /* 27-Mar-2013, tatu: Also: need to register shutdown hook to be
         *    able to do 'prepareForStop()'
         */
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                _prepareForStop();
            }
        });
    }

    public void _prepareForStop()
    {
        LOG.info("Calling prepareForStop on {} VManaged objects", _managed.size());
        for (StartAndStoppable managed : _managed) {
            try {
                managed.prepareForStop();
            } catch (Exception e) {
                LOG.warn("Problems with prepareForStop on {}: {}", managed.getClass().getName(),
                        e.getMessage());
            }
        }
        LOG.info("prepareForStop() for managed objects complete");
    }
    
    public void _stop() throws Exception
    {
        if (_managed == null) {
            LOG.error("_managed is null on _stop(): should never happen; skipping");
            return;
        }
        
        int count = _managed.size();
        LOG.info("Stopping {} managed objects", count);
        while (--count >= 0) {
            StartAndStoppable managed = _managed.remove(count);
            String desc = managed.getClass().getName();
            try {
                LOG.info("Stopping: {}", desc);
                managed.stop();
            } catch (Exception e) {
                LOG.warn(String.format("Problems trying to stop VManaged of type %s: (%s) %s",
                        desc, e.getClass().getName(), e.getMessage()),
                        e);
            }
        }
        LOG.info("VManaged object shutdown complete");
    }

    /* Ideally, shouldn't need to track this; but after having a few issues,
     * decided better safe than sorry.
     */
    protected boolean _hasBeenRun = false;
    
    @Override
    public void run(CONF dwConfig, Environment environment) throws IOException
    {
        synchronized (this) {
            if (_hasBeenRun) {
                throw new IllegalStateException("Trying to run(config, env) DWBasedService more than once");
            }
            _hasBeenRun = true;
        }
        
        // first things first: we need to get start()/stop() calls, so:
        environment.manage(new Managed() {
            @Override
            public void start() throws Exception {
                _start();
            }

            @Override
            public void stop() throws Exception {
                _stop();
            }
        });

        /* 04-Jun-2013, tatu: Goddammit, disabling gzip filter is tricky due to
         *   data-binding... Object-values get re-created. So, need to patch after
         *   the fact. And hope it works...
         *   
         * NOTE: looks like this is too late, and won't have effect. If so, modifying
         * YAML/JSON config is the only way.
         */
        dwConfig.overrideGZIPEnabled(false);

        final SCONFIG config = dwConfig.getServiceConfig();
        
        _managed = new ArrayList<StartAndStoppable>();

        StoredEntryConverter<K,E,L> entryConverter = constructEntryConverter(config, environment);
        FileManager files = constructFileManager(config);

        _serviceStuff = constructServiceStuff(config, _timeMaster,
               entryConverter, files);
        if (_runMode.isTesting()) {
            _serviceStuff.markAsTest();
        }
        
        /* Let's try opening up StorableStore: must have been created,
         * and have tables we expect; otherwise we'll fail right away.
         */
        LOG.info("Trying to open Stores (StorableStore, node store, last-access store)");
         _stores = _constructStores(_serviceStuff);
         _managed.add(_stores);
        LOG.info("Opened StorableStore successfully");
        _stores.initAndOpen(false);

        // Then: read in cluster information (config file, backend store settings):
        final int port = dwConfig.getHttpConfiguration().getPort();
        LOG.info("Initializing cluster configuration (port {})...", port);
        final long startTime = _timeMaster.currentTimeMillis();
        ClusterViewByServerImpl<K,E> cl = new ClusterBootstrapper<K,E>(startTime, _serviceStuff, _stores)
                .bootstrap(port);
        _cluster = cl;
        _managed.add(_cluster);
     
        LOG.info("Cluster configuration setup complete, with {} nodes", _cluster.size());
        
        // Index page must be done via resource, otherwise will conflict with DW/JAX-RS Servlet:
        environment.addResource(new IndexResource(loadResource("/index.html"), loadResource("/favicon.jpg")));

        // Let's first construct handlers we use:
        LOG.info("Creating handlers for service endpoints");
        ClusterInfoHandler nodeH = constructClusterInfoHandler(_serviceStuff, _cluster);
        SyncHandler<K,E> syncH = constructSyncHandler(_serviceStuff, _stores, _cluster);
        _storeHandler = constructStoreHandler(_serviceStuff, _stores, _cluster);
        _managed.add(_storeHandler);

        LOG.info("Adding service end points");
        addServiceEndpoints(_serviceStuff, environment,
                nodeH, syncH, _storeHandler);

        LOG.info("Adding health checks");
        addHealthChecks(_serviceStuff, environment);

        if (_runMode.shouldRunTasks()) {
            LOG.info("Initializing background cleaner tasks");
            _cleanerUpper = constructCleanerUpper(_serviceStuff, _stores, _cluster);
            if (_cleanerUpper != null) {
                _managed.add(_cleanerUpper);
            }
        } else {
            LOG.info("Skipping cleaner tasks for light-weight testing");
        }
        LOG.info("Initialization complete: HTTP service now running on port {}",
                dwConfig.getHttpConfiguration().getPort());
    }

    /*
    /**********************************************************************
    /* Factory methods: basic config objects
    /**********************************************************************
     */

    public boolean isTesting() { return _runMode.isTesting(); }
    
    /*
    /**********************************************************************
    /* Factory methods: basic config objects
    /**********************************************************************
     */

    /**
     * Overridable method that is used for getting helper object used for
     * constructing {@link StoredEntry} instances to store in the
     * entry metadata store.
     */
    protected abstract StoredEntryConverter<K,E,L> constructEntryConverter(SCONFIG config,
            Environment environment);

    protected abstract FileManager constructFileManager(SCONFIG serviceConfig);

    protected abstract StoresImpl<K,E> constructStores(SharedServiceStuff stuff,
            SCONFIG serviceConfig, StorableStore store,
            NodeStateStore<IpAndPort, ActiveNodeState> nodeStates);

    protected abstract SharedServiceStuff constructServiceStuff(SCONFIG serviceConfig,
            TimeMaster timeMaster, StoredEntryConverter<K,E,L> entryConverter,
            FileManager files);
    
    /*
    /**********************************************************************
    /* Factory methods for constructing handlers
    /**********************************************************************
     */

    protected abstract StoreHandler<K,E,L> constructStoreHandler(SharedServiceStuff serviceStuff,
            Stores<K,E> stores, ClusterViewByServer cluster);

    protected SyncHandler<K,E> constructSyncHandler(SharedServiceStuff stuff,
            StoresImpl<K,E> stores, ClusterViewByServerUpdatable cluster)
    {
        return new SyncHandler<K,E>(stuff, stores, cluster);
    }

    protected ClusterInfoHandler constructClusterInfoHandler(SharedServiceStuff stuff,
            ClusterViewByServerUpdatable cluster)
    {
        return new ClusterInfoHandler(stuff, cluster);
    }

    protected CleanerUpper<K,E> constructCleanerUpper(SharedServiceStuff stuff,
            Stores<K,E> stores, ClusterViewByServer cluster)
    {
        return new CleanerUpper<K,E>(_serviceStuff, _stores, _cluster,
                constructCleanupTasks());
    }

    protected abstract List<CleanupTask<?>> constructCleanupTasks();

    protected BackgroundMetricsAccessor constructMetricsAccessor(SharedServiceStuff stuff,
            ClusterViewByServer cluster, Stores<K,E> stores,
            AllOperationMetrics.Provider[] metrics) {
        return new BackgroundMetricsAccessor(stuff, stores, metrics);
    }

    /*
    /**********************************************************************
    /* Factory methods: servlets
    /**********************************************************************
     */

    protected abstract StoreEntryServlet<K,E> constructStoreEntryServlet(SharedServiceStuff stuff,
            ClusterViewByServer cluster, StoreHandler<K,E,L> storeHandler);

    protected ServletBase constructNodeStatusServlet(SharedServiceStuff stuff,
            ClusterInfoHandler nodeHandler) {
        return new NodeStatusServlet(stuff, nodeHandler);
    }

    protected ServletBase constructNodeMetricsServlet(SharedServiceStuff stuff,
            BackgroundMetricsAccessor accessor) {
        return new NodeMetricsServlet(stuff, accessor);
    }
        
    protected SyncListServlet<K,E> constructSyncListServlet(SharedServiceStuff stuff,
            ClusterViewByServer cluster, SyncHandler<K,E> syncHandler) {
        return new SyncListServlet<K,E>(stuff, cluster, syncHandler);
    }

    protected SyncPullServlet<K,E> constructSyncPullServlet(SharedServiceStuff stuff,
            ClusterViewByServer cluster, SyncHandler<K,E> syncHandler) {
        return new SyncPullServlet<K,E>(stuff, cluster, syncHandler);
    }

    protected StoreListServlet<K,E> constructStoreListServlet(SharedServiceStuff stuff,
            ClusterViewByServer cluster, StoreHandler<K,E,L> storeHandler) {
        return new StoreListServlet<K,E>(stuff, cluster, storeHandler);
    }
    
    /*
    /**********************************************************************
    /* Methods for service end point additions
    /**********************************************************************
     */

    /**
     * Method called to create service endpoints, given set of
     * handlers.
     */
    protected void addServiceEndpoints(SharedServiceStuff stuff,
            Environment environment,
            ClusterInfoHandler nodeHandler, SyncHandler<K,E> syncHandler,
            StoreHandler<K,E,L> storeHandler)
    {
        final ClusterViewByServer cluster = syncHandler.getCluster();

        EnumMap<PathType, ServletBase> servlets = new EnumMap<PathType, ServletBase>(PathType.class);
        servlets.put(PathType.NODE_STATUS, constructNodeStatusServlet(stuff, nodeHandler));

        ServletWithMetricsBase syncListServlet = constructSyncListServlet(stuff, cluster, syncHandler);
        servlets.put(PathType.SYNC_LIST, syncListServlet);
        ServletWithMetricsBase syncPullServlet = constructSyncPullServlet(stuff, cluster, syncHandler);
        servlets.put(PathType.SYNC_PULL, syncPullServlet);
        StoreEntryServlet<K,E> storeEntryServlet = constructStoreEntryServlet(stuff,
                cluster, storeHandler);
        servlets.put(PathType.STORE_ENTRY, storeEntryServlet);
        ServletWithMetricsBase storeListServlet = constructStoreListServlet(stuff,
                cluster, storeHandler);
        servlets.put(PathType.STORE_ENTRIES, storeListServlet);

        final BackgroundMetricsAccessor metrics = constructMetricsAccessor(stuff, cluster,
                storeHandler.getStores(),
                new AllOperationMetrics.Provider[] {
                    storeEntryServlet, storeListServlet, syncListServlet, syncPullServlet
                });
        servlets.put(PathType.NODE_METRICS, constructNodeMetricsServlet(stuff, metrics));

//        servlets.put(PathType.STORE_FIND_ENTRY, );
//        servlets.put(PathType.STORE_FIND_LIST, );
        
        ServiceDispatchServlet<K,E,PathType> dispatcher = new ServiceDispatchServlet<K,E,PathType>
            (cluster, null, stuff, servlets);

        RequestPathBuilder<?> rootBuilder = rootPath(stuff.getServiceConfig());
        String rootPath = servletPath(rootBuilder);
        LOG.info("Registering main Dispatcher servlet at: "+rootPath);
        environment.addServlet(dispatcher, rootPath);

        // // And finally servlet for for entry access
        
        addStoreEntryServlet(stuff, environment, cluster, _storeHandler);
    }
    
    /**
     * Overridable method used for hooking standard entry access endpoint into
     * alternate location. Usually used for backwards compatibility purposes.
     */
    protected void addStoreEntryServlet(SharedServiceStuff stuff,
            Environment environment,
            ClusterViewByServer cluster,
            StoreHandler<K,E,?> storeHandler)
    {
    }
    
    protected void addHealthChecks(SharedServiceStuff stuff,
            Environment environment) { }
    
    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */
    
    protected StoresImpl<K,E> _constructStores(SharedServiceStuff stuff)
        throws IOException
    {
        final SCONFIG v = stuff.getServiceConfig();
        StoreBackendBuilder<?> b = v.instantiateBackendBuilder();
        StoreBackendConfig backendConfig = v._storeBackendConfigOverride;
        if (backendConfig == null) { // no overrides, use databinding
            Class<? extends StoreBackendConfig> cfgType = b.getConfigClass();
            if (v.storeBackendConfig == null) {
                throw new IllegalStateException("Missing 'v.storeBackendConfig");
            }
            backendConfig = stuff.convertValue(v.storeBackendConfig, cfgType);
        }
        b = b.with(v.storeConfig)
                .with(backendConfig);
        StoreBackend backend = b.build();
        StorableStore store = new StorableStoreImpl(v.storeConfig, backend, _timeMaster,
               stuff.getFileManager(),
               _constructThrottler(stuff), _constructWriteMutex(stuff));
        NodeStateStore<IpAndPort, ActiveNodeState> nodeStates = _buildNodeStateStore(stuff, b);
        return constructStores(stuff, v, store, nodeStates);
    }

    protected NodeStateStore<IpAndPort, ActiveNodeState> _buildNodeStateStore(SharedServiceStuff stuff,
            StoreBackendBuilder<?> backendBuilder)
    {
        /* 09-Dec-2013, tatu: Now we will also construct NodeStateStore using
         *   the very same builder...
         */
        ObjectMapper mapper = stuff.jsonMapper();
        File root = stuff.getServiceConfig().metadataDirectory;
        return backendBuilder.<IpAndPort, ActiveNodeState>buildNodeStateStore(root,
                        new JacksonBasedConverter<IpAndPort>(mapper, IpAndPort.class),
                        new JacksonBasedConverter<ActiveNodeState>(mapper, ActiveNodeState.class));
    }
    
    /**
     * Factory method called to instantiate {@link StoreOperationThrottler}
     * to use for throttling underlying local database operations.
     * If null is returned, store is free to use whatever default throttling
     * mechanism it needs to for ensuring consistency, but nothing more.
     *<p>
     * Default implementation simply returns null to let the default throttler
     * be used.
     */
    protected StoreOperationThrottler _constructThrottler(SharedServiceStuff stuff)
    {
        // null -> use the default implementation
        return null;
    }

    /**
     * Factory method called to instantiate {@link PartitionedWriteMutex}
     * to use for ensuring atomicity of read-modify-write operations.
     * If null is returned, store is free to use its default implementation.
     *<p>
     * Default implementation simply returns null to let the default implementation
     * be used.
     */
    protected PartitionedWriteMutex _constructWriteMutex(SharedServiceStuff stuff)
    {
        // null -> use the default implementation
        return null;
    }
    
    /*
    /**********************************************************************
    /* Accessors for tests
    /**********************************************************************
     */

    public TimeMaster getTimeMaster() {
        return _timeMaster;
    }
    
    /*
    /**********************************************************************
    /* Helper methods
    /**********************************************************************
     */

    protected JdkHttpClientPathBuilder rootPath(ServiceConfig config)
    {
        return new JdkHttpClientPathBuilder("localhost")
            .addPathSegments(config.servicePathRoot);
    }

    /**
     * Helper method for constructing Servlet registration path, given
     * a basic end point path definition. Currently just verifies prefix
     * and suffix slashes and adds '*' as necessary.
     */
    protected String servletPath(RequestPathBuilder<?> pathBuilder)
    {
        String base = pathBuilder.getPath();
        if (!base.endsWith("*")) {
            if (base.endsWith("/")) {
                base += "*";
            } else {
                base += "/*";
            }
        }
        if (!base.startsWith("/")) {
            base = "/"+base;
        }
        return base;
    }

    protected byte[] loadResource(String ref) throws IOException
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(4000);
        InputStream in = getClass().getResourceAsStream(ref);
        byte[] buffer = new byte[4000];
        int count;
     
        while ((count = in.read(buffer)) > 0) {
            bytes.write(buffer, 0, count);
        }
        in.close();
        byte[] data = bytes.toByteArray();
        if (data.length == 0) {
            String msg = "Could not find resource '"+ref+"'";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        return bytes.toByteArray();
    }
}
