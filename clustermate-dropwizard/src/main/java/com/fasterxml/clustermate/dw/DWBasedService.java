package com.fasterxml.clustermate.dw;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.util.component.LifeCycle.Listener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.fasterxml.clustermate.api.RequestPathBuilder;
import com.fasterxml.clustermate.jaxrs.IndexResource;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.cleanup.CleanerUpper;
import com.fasterxml.clustermate.service.cleanup.CleanupTask;
import com.fasterxml.clustermate.service.cluster.*;
import com.fasterxml.clustermate.service.state.ActiveNodeState;
import com.fasterxml.clustermate.service.state.JacksonBasedConverter;
import com.fasterxml.clustermate.service.store.*;
import com.fasterxml.clustermate.service.sync.SyncHandler;
import com.fasterxml.clustermate.servlet.*;
import com.fasterxml.clustermate.std.JdkHttpClientPathBuilder;

public abstract class DWBasedService<
    K extends EntryKey,
    E extends StoredEntry<K>,
    SCONFIG extends ServiceConfig,
    CONF extends DWConfigBase<SCONFIG, CONF>
>
    extends Application<CONF>
{
    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    /*
    /**********************************************************************
    /* Basic configuration
    /**********************************************************************
     */

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
     * This object is needed to allow test code to work around usual
     * waiting time restrictions.
     */
    protected final TimeMaster _timeMaster;

    protected SCONFIG _config;
    
    /*
    /**********************************************************************
    /* State management
    /**********************************************************************
     */
    
    /**
     * Container for various stores we use for data, metadata.
     */
    protected StoresImpl<K,E> _stores;

    /**
     * And we better hang on to cluster view as well
     */
    protected ClusterViewByServerUpdatable _cluster;
    
    /*
    /**********************************************************************
    /* Service handlers
    /**********************************************************************
     */

    protected ClusterInfoHandler _clusterInfoHandler;

    protected SyncHandler<K,E> _syncHandler;
    
    protected StoreHandler<K,E,?> _storeHandler;

    /*
    /**********************************************************************
    /* Background processes
    /**********************************************************************
     */
    
    /**
     * Manager object that deals with data expiration and related
     * clean up tasks.
     */
    protected CleanerUpper<K,E> _cleanerUpper;
    
    /*
    /**********************************************************************
    /* State
    /**********************************************************************
     */
    
    /**
     * List of {@link StartAndStoppable} objects we will dispatch start/stop calls to.
     */
    protected List<StartAndStoppable> _managed = null;
    
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
        LOG.info("Starting up {} Managed objects", _managed.size());
        for (StartAndStoppable managed : _managed) {
            LOG.info("Starting up: {}", managed.getClass().getName());
            try {
                managed.start();
            } catch (Exception e) {
                LOG.warn("Problems starting component {}: {}", _managed.getClass().getName(), e);
            }
        }
        LOG.info("Managed object startup complete");

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

    protected final AtomicBoolean _prepareForStopCalled = new AtomicBoolean(false);
    
    public void _prepareForStop()
    {
        // only call once:
        if (!_prepareForStopCalled.compareAndSet(false, true)) {
            return;
        }
        LOG.info("Calling prepareForStop on {} Managed objects", _managed.size());
        for (StartAndStoppable managed : _managed) {
            try {
                managed.prepareForStop();
            } catch (Exception e) {
                LOG.warn("Problems with prepareForStop on {}: {}", managed.getClass().getName(),
                        e.getMessage());
            }
        }
        LOG.info("prepareForStop() for Managed objects complete");
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
                LOG.warn(String.format("Problems trying to stop Managed of type %s: (%s) %s",
                        desc, e.getClass().getName(), e.getMessage()),
                        e);
            }
        }
        LOG.info("Managed object shutdown complete");
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
        Listener l = new Listener() {
            @Override
            public void lifeCycleStarting(LifeCycle event) {
                try {
                    _start();
                } catch (Exception e) {
                    LOG.warn("Problems starting components: {}", e);
                }
            }

            @Override
            public void lifeCycleStarted(LifeCycle event) {
            }

            @Override
            public void lifeCycleFailure(LifeCycle event, Throwable cause) {
                // what to do here, if anything?
            }

            @Override
            public void lifeCycleStopping(LifeCycle event) {
                try {
                    _prepareForStop();
                } catch (Exception e) {
                    LOG.warn("Problems preparing components for stopping: {}", e);
                }
            }

            @Override
            public void lifeCycleStopped(LifeCycle event) {
                try {
                    _stop();
                } catch (Exception e) {
                    LOG.warn("Problems stopping components: {}", e);
                }
            }
        };

        environment.getApplicationContext().addLifeCycleListener(l);
        
        _config = dwConfig.getServiceConfig();
        
        /* 04-Jun-2013, tatu: Goddammit, disabling gzip filter is tricky due to
         *   data-binding... Object-values get re-created. So, need to patch after
         *   the fact. And hope it works...
         *   
         * NOTE: looks like this is too late, and won't have effect. If so, modifying
         * YAML/JSON config is the only way.
         */
        dwConfig.overrideGZIPEnabled(false);
        
        _managed = new ArrayList<StartAndStoppable>();
        _serviceStuff = constructServiceStuff(_config, _timeMaster, constructEntryConverter(),
                constructFileManager());
        if (_runMode.isTesting()) {
            _serviceStuff.markAsTest();
        }
        
        /* Let's try opening up StorableStore: must have been created,
         * and have tables we expect; otherwise we'll fail right away.
         */
        LOG.info("Trying to open Stores (StorableStore, node store, last-access store)");
        _stores = constructStores();
        _managed.add(_stores);
        LOG.info("Opened StorableStore successfully");
        _stores.initAndOpen(false);

        // Then: read in cluster information (config file, backend store settings):
        final int port = dwConfig.getApplicationPort();
        LOG.info("Initializing cluster configuration (port {})...", port);
        final long startTime = _timeMaster.currentTimeMillis();
        ClusterViewByServerImpl<K,E> cl = new ClusterBootstrapper<K,E>(startTime, _serviceStuff, _stores)
                .bootstrap(port);
        _cluster = cl;
        _managed.add(_cluster);
     
        LOG.info("Cluster configuration setup complete, with {} nodes", _cluster.size());
        
        // Index page must be done via resource, otherwise will conflict with DW/JAX-RS Servlet:
        environment.jersey().register(new IndexResource(loadResource("/index.html"),
                loadResource("/favicon.jpg")));

        // Let's first construct handlers we use:
        LOG.info("Creating handlers for service endpoints");
        _clusterInfoHandler = constructClusterInfoHandler();
        _syncHandler = constructSyncHandler();
        _storeHandler = constructStoreHandler();
        _managed.add(_storeHandler);

        LOG.info("Adding service end points");
        addServiceEndpoints(environment, constructServletFactory());

        LOG.info("Adding health checks");
        addHealthChecks(environment);

        if (_runMode.shouldRunTasks()) {
            LOG.info("Initializing background cleaner tasks");
            _cleanerUpper = constructCleanerUpper();
            if (_cleanerUpper != null) {
                _managed.add(_cleanerUpper);
            }
        } else {
            LOG.info("Skipping cleaner tasks for light-weight testing");
        }
        LOG.info("Initialization complete: HTTP service now running on port {}",
                dwConfig.getApplicationPort());
    }
    
    /*
    /**********************************************************************
    /* Accessors
    /**********************************************************************
     */

    public boolean isTesting() { return _runMode.isTesting(); }

    protected SCONFIG serviceConfig() {
        return _config;
    }

    // For tests:
    public TimeMaster getTimeMaster() {
        return _timeMaster;
    }
    
    /*
    /**********************************************************************
    /* Factory methods: basic bootstrap config objects.
    /**********************************************************************
     */

    /**
     * Overridable method that is used for getting helper object used for
     * constructing {@link StoredEntry} instances to store in the
     * entry metadata store.
     */
    @SuppressWarnings("unchecked")
    protected StoredEntryConverter<K,E,?> constructEntryConverter() {
        return (StoredEntryConverter<K,E,?>) serviceConfig().getEntryConverter();
    }

    protected abstract FileManager constructFileManager();

    protected abstract StoresImpl<K,E> constructStores(StorableStore store,
            NodeStateStore<IpAndPort, ActiveNodeState> nodeStates);

    protected abstract SharedServiceStuff constructServiceStuff(SCONFIG serviceConfig,
            TimeMaster timeMaster, StoredEntryConverter<K,E,?> entryConverter,
            FileManager files);
    
    /*
    /**********************************************************************
    /* Factory methods for constructing handlers
    /**********************************************************************
     */

    protected abstract StoreHandler<K,E,?> constructStoreHandler();

    protected SyncHandler<K,E> constructSyncHandler() {
        return new SyncHandler<K,E>(_serviceStuff, _stores, _cluster);
    }

    protected ClusterInfoHandler constructClusterInfoHandler() {
        return new ClusterInfoHandler(_serviceStuff, _cluster);
    }

    protected CleanerUpper<K,E> constructCleanerUpper() {
        return new CleanerUpper<K,E>(_serviceStuff, _stores, _cluster,
                constructCleanupTasks());
    }

    protected abstract List<CleanupTask<?>> constructCleanupTasks();

    /*
    /**********************************************************************
    /* Methods for service end point additions
    /**********************************************************************
     */

    protected abstract CMServletFactory constructServletFactory();

    /*
    protected CMServletFactory constructServletFactory() {
        return new DefaultCMServletFactory<K,E,SCONFIG>(_serviceStuff,
                _stores, _cluster, _clusterInfoHandler, _syncHandler, _storeHandler);
    }
    */
    
    /**
     * Method called to create service endpoints, given set of
     * handlers.
     */
    protected void addServiceEndpoints(Environment environment, CMServletFactory servletFactory)
    {
        RequestPathBuilder<?> rootBuilder = rootPath(_serviceStuff.getServiceConfig());
        String rootPath = servletPath(rootBuilder);
        LOG.info("Registering main Dispatcher servlet at: "+rootPath);
        ServletBase dispatcher = servletFactory.contructDispatcherServlet();
        if (dispatcher != null) {
            environment.servlets()
                .addServlet("CM-Dispatcher", dispatcher)
                .addMapping(rootPath);
        }
        // // And optional additional servlet for for entry access
        addStoreEntryServlet(environment);
    }
    
    /**
     * Overridable method used for hooking standard entry access endpoint into
     * alternate location. Usually used for backwards compatibility purposes.
     */
    protected void addStoreEntryServlet(Environment environment) { }

    /**
     * Method called for installing DropWizard-mandated health checks.
     */
    protected void addHealthChecks(Environment environment) { }
    
    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */
    
    protected StoresImpl<K,E> constructStores() throws IOException
    {
        final SCONFIG sconfig = serviceConfig();
        StoreBackendBuilder<?> b = sconfig.instantiateBackendBuilder();
        StoreBackendConfig backendConfig = sconfig._storeBackendConfigOverride;
        if (backendConfig == null) { // no overrides, use databinding
            Class<? extends StoreBackendConfig> cfgType = b.getConfigClass();
            if (sconfig.storeBackendConfig == null) {
                throw new IllegalStateException("Missing 'config.storeBackendConfig");
            }
            backendConfig = _serviceStuff.convertValue(sconfig.storeBackendConfig, cfgType);
        }
        b = b.with(sconfig.storeConfig)
                .with(backendConfig);
        StoreBackend backend = b.build();
        StorableStore store = new StorableStoreImpl(sconfig.storeConfig, backend, _timeMaster,
                _serviceStuff.getFileManager(),
               constructThrottler(), constructWriteMutex());
        NodeStateStore<IpAndPort, ActiveNodeState> nodeStates = constructNodeStateStore(b);
        return constructStores(store, nodeStates);
    }

    protected NodeStateStore<IpAndPort, ActiveNodeState> constructNodeStateStore(StoreBackendBuilder<?> backendBuilder)
    {
        /* 09-Dec-2013, tatu: Now we will also construct NodeStateStore using
         *   the very same builder...
         */
        ObjectMapper mapper = _serviceStuff.jsonMapper();
        File root = _serviceStuff.getServiceConfig().metadataDirectory;
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
    protected StoreOperationThrottler constructThrottler() {
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
    protected PartitionedWriteMutex constructWriteMutex() {
        // null -> use the default implementation
        return null;
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
