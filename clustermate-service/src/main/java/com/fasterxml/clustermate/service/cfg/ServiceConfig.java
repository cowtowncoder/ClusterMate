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
    public String[] servicePathRoot = new String[] { "v" };

    /**
     * Method to find {@link RequestPathStrategy} used for matching
     * request paths to resources.
     */
    public abstract RequestPathStrategy getServicePathStrategy();

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
    /* Storage config: paths, helper classes
    /**********************************************************************
     */
  
    /**
     * Directory used for storing service metadata, such as node
     * states, last-accessed timestamps.
     */
    @NotNull
    public File metadataDirectory;

    /**
     * What is the grace period for syncing: that is, how many seconds do we give
     * for servers to get content from clients before we try to synchronize.
     * This balances fast synchronization with overhead of doing sync: for now,
     * let's give 60 seconds.
     */
    public TimeSpan cfgSyncGracePeriod = new TimeSpan("60s");

    /*
    /**********************************************************************
    /* Storage config: data expiration
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
     * By default we will not run cleanup more often than once per hour.
     * The first cleanup will typically be run earlier than delay.
     */
    @NotNull
    public TimeSpan cfgDelayBetweenCleanup = new TimeSpan("60m");

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
     * This value specifies time that tombstones (deletion markers) should
     * be kept in the metadata store before purging. Delay in deletion is
     * necessary to ensure proper deletion across instance during outages.
     *<p>
     * Current default value (1 hour) is chosen to balance eventual consistency
     * (when some nodes fail) with additional storage cost.
     */
    public TimeSpan cfgTombstoneTTL = new TimeSpan("1h");

    /**
     * How many entries can we request with each call sync-list call?
     * Responses are fully buffered in memory, so let's say... 500?
     */
    public int cfgMaxEntriesPerSyncList = 500;
    
    /*
    /**********************************************************************
    /* StoreMate (single-node storage subsystem V uses) configs
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
    /* Construction
    /**********************************************************************
     */

    protected ServiceConfig() { }

    protected ServiceConfig(String[] root) {
        servicePathRoot = root;
    }
    
    /*
    /**********************************************************************
    /* Other access methods
    /**********************************************************************
     */

    public abstract StoredEntryConverter<?,?> getEntryConverter();

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
