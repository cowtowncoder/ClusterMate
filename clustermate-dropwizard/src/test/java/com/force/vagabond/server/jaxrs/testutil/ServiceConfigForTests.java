package com.force.vagabond.server.jaxrs.testutil;

import com.fasterxml.clustermate.api.RequestPathStrategy;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;
import com.fasterxml.storemate.store.backend.StoreBackendConfig;

/**
 * Configuration Object used for tests.
 */
public class ServiceConfigForTests
    extends ServiceConfig
{
    // not a big deal since we don't use real client; otherwise would
    // need to match
    protected final static String[] TEST_SVC_ROOT = new String[] { "cmtest" };
    
    protected RequestPathStrategy _requestPathStrategy;
    protected String defaultPartition;

    public ServiceConfigForTests() {
        this(new PathsForTests());
    }

    public ServiceConfigForTests(RequestPathStrategy paths) {
        super(TEST_SVC_ROOT);
        _requestPathStrategy = paths;
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Abstract method impl
    ///////////////////////////////////////////////////////////////////////
     */
    
    @Override
    public RequestPathStrategy getServicePathStrategy() {
        return _requestPathStrategy;
    }
    
    @Override
    public StoredEntryConverter<?,?> getEntryConverter() {
        PartitionId defClientId = getDefaultPartition();
        return new StoredEntryConverterForTests(TestKeyConverter.defaultInstance(defClientId));
    }
    /*
    ///////////////////////////////////////////////////////////////////////
    // Accessors
    ///////////////////////////////////////////////////////////////////////
     */

    public PartitionId getDefaultPartition() throws IllegalArgumentException
    {
        if (defaultPartition == null
                || "".equals(defaultPartition)) {
            return null;
        }
        PartitionId cid = PartitionId.valueOf(defaultPartition);
        return (cid.asInt() == 0) ? null : cid;
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Additional mutators
    ///////////////////////////////////////////////////////////////////////
     */

    public ServiceConfigForTests overrideStoreBackendConfig(StoreBackendConfig cfg) {
        _storeBackendConfigOverride = cfg;
        return this;
    }

    public ServiceConfigForTests overrideDefaultPartition(PartitionId def) {
        defaultPartition = (def == null) ? null : def.toString();
        return this;
    }
}
