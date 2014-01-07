package com.fasterxml.clustermate.service.cfg;

import java.io.File;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.skife.config.TimeSpan;

import com.fasterxml.clustermate.api.RequestPathStrategy;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;
import com.fasterxml.storemate.store.StoreConfig;
import com.fasterxml.storemate.store.backend.StoreBackendBuilder;
import com.fasterxml.storemate.store.backend.StoreBackendConfig;
import com.fasterxml.storemate.store.lastaccess.LastAccessConfig;

/**
 * Configuration Object that ClusterMate-based service uses: either read from
 * a JSON or YAML file, or programmatically constructed.
 * Often aggregated in a container-specific wrapper object (like one used by
 * DropWizard).
 */
public abstract class ServiceConfig
{
    /*
    /**********************************************************************
    /* Service end point registration
    /**********************************************************************
     */

    /**
     * Root path used for registering all the end points of the system,
     * using configurable 
     */
    @NotNull
    public String[] servicePathRoot;

    /**
     * Method to find {@link RequestPathStrategy} used for matching
     * request paths to resources.
     */
    public abstract RequestPathStrategy<?> getServicePathStrategy();
    
    /*
    /**********************************************************************
    /* Cluster configuration
    /**********************************************************************
     */

    @NotNull
    @Valid
    public ClusterConfig cluster = new ClusterConfig();

    /*
    /**********************************************************************
    /* Storage config: StoreMate (single-node storage subsystem) configs
    /**********************************************************************
     */

    /**
     * General configuration for the underlying entry metadata store.
     */
    public StoreConfig storeConfig = new StoreConfig();

    /**
     * Type of the store backend, specified using type of builder
     * for 
     */
    public Class<? extends StoreBackendBuilder<?>> storeBackendType;
    
    /**
     * Configuration settings for the store backend: specific type depends on
     * backend type (class), and data-binding is done lazily.
     */
    public java.util.Map<String,Object> storeBackendConfig;

    /**
     * Alternatively, instead of "raw" JSON, it is possible to just give overrides
     * for backend. Used by some tests.
     */
    public StoreBackendConfig _storeBackendConfigOverride;
    
    /*
    /**********************************************************************
    /* Storage config: paths
    /**********************************************************************
     */
  
    /**
     * Directory used for storing service metadata, such as node
     * states, last-accessed timestamps.
     */
    @NotNull
    public File metadataDirectory;

    /*
    /**********************************************************************
    /* Storage config: last-access store
    /**********************************************************************
     */

    /**
     * Configuration of the last-accessed store, where optional
     * last-accessed information is stored (if used).
     */
    public LastAccessConfig lastAccess = new LastAccessConfig();

    /*
    /**********************************************************************
    /* Entry Store behavior
    /**********************************************************************
     */
    
    /**
     * Configuration setting that determines whether it is legal to
     * re-create a formerly deleted entry that still has a tombstone.
     * If allowed (true), entries can be recreated if (and only if)
     * content hashes match; if not allowed (false), 410 Gone
     * response will be returned if PUT is attempted on tombstone
     * entry (something DELETEd recently, not yet cleaned up).
     */
    public boolean cfgAllowUndelete = false;

    /**
     * Configuration setting that determines whether deleted entries
     * with tombstone are reported as empty entries (204, No Content)
     * or as missing (404, Not Found): if false, 404 returned,
     * if true, 204.
     */
    public boolean cfgReportDeletedAsEmpty = true;

    /**
     * DELETE operations may be deferred; and if so, here's configuration
     * for details.
     */
    public DeferredDeleteConfig deletes = new DeferredDeleteConfig();
    
    /*
    /**********************************************************************
    /* Storage config: data expiration, deletion, cleanup
    /**********************************************************************
     */

    /**
     * This value specifies absolute maximum time-to-live value for any
     * entry stored in system; this is a hard value which will not
     * take into account last-accessed timestamp.
     *<p>
     * By default this is set to 14 days (2 weeks)
     */
    public TimeSpan cfgMaxMaxTTL = new TimeSpan("14d");
    
    /**
     * This value specifies the default maximum time-to-live value for any
     * entry stored in system without specifying per-item maximum TTL.
     *<p>
     * Default value is 7 days (1 week)
     */
    public TimeSpan cfgDefaultMaxTTL = new TimeSpan("14d");

    /**
     * This value specifies the maximum value that can defined for
     * "TTL since last-access".
     *<p>
     * Default value is 2 days.
     */
    public TimeSpan cfgMaxSinceAccessTTL = new TimeSpan("2d");

    /**
     * This value specifies the default "time-to-live since last access"
     * value for any entry stored in system without specifying per-item value.
     * In cases where access-time is not tracked, this basically means
     * "time-to-live since creation", as creation is considered baseline
     * for "last-access".
     *<p>
     * Note that this setting can not override value of "maximum TTL", that is,
     * it can only make entries live to at most "maximum TTL". It does however
     * apply above and beyond "minimum TTL" (which would otherwise let 'old enough'
     * entries be expired)
     *<p>
     * Default value of 3 hours, then, means that by default no entry will expire
     * less than 3 hours after last access or creation.
     */
    public TimeSpan cfgDefaultSinceAccessTTL = new TimeSpan("3h");
    
    /**
     * By default we will not run cleanup more often than about once
     * an hour.
     * But the first cleanup will typically be run earlier than delay.
     *<p>
     * Let's use a value that is NOT neatly divisible, just to try to 
     * avoid syncing up to some schedule.
     */
    @NotNull
    public TimeSpan cfgDelayBetweenCleanup = new TimeSpan("40m");
    
    /**
     * This value specifies time that tombstones (deletion markers) should
     * be kept in the metadata store before purging. Delay in deletion is
     * necessary to ensure proper deletion across instance during outages.
     *<p>
     * Current default value (45 minutes) is chosen to balance eventual consistency
     * (when some nodes fail) with additional storage cost.
     */
    public TimeSpan cfgTombstoneTTL = new TimeSpan("45m");

    /*
    /**********************************************************************
    /* Storage config: synchronization settings
    /**********************************************************************
     */

    /**
     * What is the grace period for syncing: that is, how many seconds do we give
     * for servers to get content from clients before we try to synchronize.
     * This balances fast synchronization with overhead of doing sync: for now,
     * let's give 20 seconds.
     */
    public TimeSpan cfgSyncGracePeriod = new TimeSpan("20s");
    
    /**
     * How many entries can we request with each call sync-list call?
     * Responses are fully buffered in memory, so let's say... 500?
     */
    public int cfgMaxEntriesPerSyncList = 500;

    /**
     * What is the maximum amount of time server may keep connection
     * for "Sync List" open before having to return empty result
     * set (non-empty result sets are to be returned right away without
     * further waiting). This means that servers may put request to sleep
     * for up to this amount of time, but only if it does not yet have
     * any results to return.
     *<br />
     * NOTE: should be kept relatively low; server will let client know
     * of additional sleep it may do before retrying sync list request.
     */
    public TimeSpan cfgSyncMaxLongPollTime = new TimeSpan("3s");

    /*
    /**********************************************************************
    /* Metrics settings
    /**********************************************************************
     */

    /**
     * Setting that determines whether Yammer metrics information will be
     * updated or not.
     */
    public boolean metricsEnabled = true;

    /**
     * Root name for metrics properties when reported via JMX.
     */
    public String metricsJmxRoot = "com.fasterxml.clustermate.metrics";
    
    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    protected ServiceConfig(String[] root) {
        servicePathRoot = root;
    }

    /*
    /**********************************************************************
    /* Programmatic overrides
    /**********************************************************************
     */

    public ServiceConfig overrideSyncGracePeriod(String periodDesc) {
        try {
            cfgSyncGracePeriod = new TimeSpan(periodDesc);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid delay definition '"+periodDesc+"': "+e.getMessage());
        }
        return this;
    }

    public ServiceConfig overrideMaxLongPollTime(String periodDesc) {
        try {
            cfgSyncMaxLongPollTime = new TimeSpan(periodDesc);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid max setting definition '"+periodDesc+"': "+e.getMessage());
        }
        return this;
    }

    public ServiceConfig overrideDefaultMaxTTL(String periodDesc) {
        try {
            cfgDefaultMaxTTL = new TimeSpan(periodDesc);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid max TTL definition '"+periodDesc+"': "+e.getMessage());
        }
        return this;
    }
    
    public ServiceConfig overrideDefaultSinceAccessTTL(String periodDesc) {
        try {
            cfgDefaultSinceAccessTTL = new TimeSpan(periodDesc);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid min TTL definition '"+periodDesc+"': "+e.getMessage());
        }
        return this;
    }
    
    public ServiceConfig overrideStoreBackendConfig(StoreBackendConfig cfg) {
        _storeBackendConfigOverride = cfg;
        return this;
    }
    
    /*
    /**********************************************************************
    /* Other access methods
    /**********************************************************************
     */

    public abstract StoredEntryConverter<?,?,?> getEntryConverter();

    public StoreBackendBuilder<?> instantiateBackendBuilder()
    {
        if (storeBackendType == null) {
            throw new IllegalStateException("Missing configuration for 'storeBackendType'");
        }
        return _createInstance(storeBackendType, "storeBackendType",
                StoreBackendBuilder.class);
    }    
    
    @SuppressWarnings("unchecked")
    protected <T> T _createInstance(Class<?> implCls, String desc,
            Class<T> baseType)
    {
        // verify first; not 100% confident annotation verifications catch everything
        if (!baseType.isAssignableFrom(implCls)) {
            throw new IllegalStateException("Invalid value for '"+desc+"': "
                    +implCls.getName()+" is not a sub-type of "+baseType.getName());
        }
        try {
            // TODO: find the constructor, call 'setAccessible' if necessary?
            return (T) implCls.newInstance();
        } catch (Exception e) {
            Throwable t = e;
            while (t.getCause() != null) {
                t = t.getCause();
            }
            throw new IllegalStateException("Could not instantiate property '"+desc+"', of type "
                    +implCls.getName()+". Problem ("+t.getClass().getName()+"): "+t.getMessage(), t);
        }
    }
}
