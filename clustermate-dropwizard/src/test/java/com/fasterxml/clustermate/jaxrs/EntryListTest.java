package com.fasterxml.clustermate.jaxrs;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.clustermate.service.store.StoredEntry;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.ListType;
import com.fasterxml.clustermate.api.msg.ListResponse;
import com.fasterxml.clustermate.jaxrs.testutil.*;
import com.fasterxml.clustermate.json.ClusterMateObjectMapper;

public class EntryListTest extends JaxrsStoreTestBase
{
    final static PartitionId CLIENT_ID = PartitionId.valueOf(1234);

    final static String GROUP1 = "groupA";
    final static String GROUP2 = "groupB";

    final static byte[] SMALL_DATA = new byte[] { 1, 2, 3, 4, 5 };

    final static ObjectMapper MAPPER = new ClusterMateObjectMapper();
    
    @Override
    public void setUp() {
        initTestLogging();
    }
    
    public void testSimpleEntryList() throws Exception
    {
        final long creationTime = 1234L;
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(creationTime);

        StoreResourceForTests<TestKey, StoredEntry<TestKey>> resource = createResource("listSimple", timeMaster, true);
        
        // First, add couple of entries
        StorableStore entries = resource.getStores().getEntryStore();

        _addEntry(resource, GROUP1, "foo");
        _addEntry(resource, GROUP1, "bar");
        _addEntry(resource, GROUP2, "more");
        _addEntry(resource, GROUP2, "data");
        _addEntry(resource, null, "abcdef");

        assertEquals(5, entries.getEntryCount());

        // and then see what we can see, for group 1; should see 2 entries first
        FakeHttpResponse response = new FakeHttpResponse();
        FakeHttpRequest request = new FakeHttpRequest()
            .addQueryParam(ClusterMateConstants.HTTP_QUERY_PARAM_TYPE, ListType.ids.toString());
        resource.getHandler().listEntries(request, response, contentKey(CLIENT_ID, GROUP1, "a"), null);
        
        if (response.getStatus() != 200) {
            fail("Failed to list (status "+response.getStatus()+"), entity: "+response.getEntity());
        }
        assertEquals(200, response.getStatus());
        assertTrue(response.hasStreamingContent());
        byte[] stuff = this.collectOutput(response);
        ListResponse.IdListResponse resultList = MAPPER.readValue(stuff, ListResponse.IdListResponse.class);

        assertNotNull(resultList);
        assertNotNull(resultList.items);
        assertEquals(2, resultList.items.size());

        // clean up:
        resource.getStores().stop();
    }

    private void _addEntry(StoreResourceForTests<TestKey, StoredEntry<TestKey>> resource,
            String group, String path) throws IOException
    {
        final TestKey KEY = contentKey(CLIENT_ID, group, path);

        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, KEY);
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response, KEY,
                calcChecksum(SMALL_DATA), new ByteArrayInputStream(SMALL_DATA),
                null, null, null);
        assertEquals(200, response.getStatus());
    }   
}
