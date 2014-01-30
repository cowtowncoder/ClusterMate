package com.fasterxml.clustermate.jaxrs.common;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.backend.BackendStats;
import com.fasterxml.storemate.store.backend.BackendStatsConfig;
import com.fasterxml.clustermate.jaxrs.StoreResource;
import com.fasterxml.clustermate.jaxrs.testutil.TestKey;
import com.fasterxml.clustermate.jaxrs.testutil.TimeMasterForSimpleTesting;
import com.fasterxml.clustermate.service.store.StoredEntry;

public abstract class StatsTestBase extends JaxrsStoreTestBase
{
    @Override
    public void setUp() {
        initTestLogging();
    }

    protected abstract String testPrefix();
    
    public void testSimpleStatsAccess() throws Exception
    {
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(100);
        StoreResource<TestKey, StoredEntry<TestKey>> resource = createResource(testPrefix(), timeMaster, true);
        // ok: assume empty Entity Store
        StorableStore entries = resource.getStores().getEntryStore();
        assertEquals(0, entryCount(entries));

        BackendStats stats = entries.getBackend().getEntryStatistics(BackendStatsConfig.DEFAULT);
        assertNotNull(stats);

        /* NOTE: BDB stats are messy, require handling; this is copied from
         * BackgroundMetricsAccessor...
         */
        if ("bdb".equals(stats.getType())) {
//            stats = new CleanBDBStats((BDBBackendStats) stats);
        }
        
        ObjectMapper mapper = new ObjectMapper();
        String json = null;

        try {
            json = mapper.writeValueAsString(stats);
        } catch (JsonMappingException e) {
            Throwable t = e;
            while (t.getCause() != null) {
                t = t.getCause();
            }
            t.printStackTrace();
            fail("Serialization failed due to: "+e.getMessage()+"; from: "+t);
        }
        assertNotNull(json);
        
        entries.stop();
    }
}
