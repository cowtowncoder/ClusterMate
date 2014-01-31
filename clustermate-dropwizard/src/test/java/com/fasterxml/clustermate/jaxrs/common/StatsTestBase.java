package com.fasterxml.clustermate.jaxrs.common;

import java.util.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

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

        try {
            assertEquals(0, entryCount(entries));
    
            BackendStats stats = entries.getBackend().getEntryStatistics(BackendStatsConfig.DEFAULT);
            assertNotNull(stats);
    
            /* NOTE: BDB stats are messy, and for real use require scrubbing to remove cruft.
             * However, even without it, serialization should work, so what we test here
             * is really just lack of failures plus existence of certain basic set of data.
             */
            
            ObjectMapper mapper = new ObjectMapper();
            JsonNode json = mapper.valueToTree(stats);
            assertNotNull(json);
            assertTrue(json.isObject());

            // Ok, first verify std stats
            ObjectNode rootOb = (ObjectNode) json;
            _verifyExistenceOf(rootOb, "creationTime");
            _verifyExistenceOf(rootOb, "timeTakenMsecs");
            _verifyExistenceOf(rootOb, "type");

            // then delegate to backend-specific testing
            _verifyStatsJson(rootOb);
        } finally {
            entries.stop();
        }
    }

    protected abstract void _verifyStatsJson(ObjectNode json);

    protected void _verifyExistenceOf(ObjectNode json, String key)
    {
        if (!json.has(key)) {
            Iterator<String> it = json.fieldNames();
            List<String> keys = new ArrayList<String>();
            while (it.hasNext()) {
                keys.add(it.next());
            }
            fail("No '"+key+"' found in expected place; only entries seen = "+keys);
        }
        
    }
}
