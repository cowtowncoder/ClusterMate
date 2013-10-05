package com.fasterxml.clustermate.jaxrs;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.clustermate.service.store.StoredEntry;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.ListItemType;
import com.fasterxml.clustermate.api.msg.ListItem;
import com.fasterxml.clustermate.api.msg.ListResponse;
import com.fasterxml.clustermate.jaxrs.testutil.*;
import com.fasterxml.clustermate.json.ClusterMateObjectMapper;

public abstract class EntryListTestBase extends JaxrsStoreTestBase
{
    final static CustomerId CUSTOMER_ID1 = CustomerId.valueOf(1234);
    final static CustomerId CUSTOMER_ID2 = CustomerId.valueOf(4567);
    final static CustomerId CUSTOMER_ID3 = CustomerId.valueOf(6666);

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

        TestKey key1 = _addEntry(resource, CUSTOMER_ID1, "foo");
        TestKey key2 = _addEntry(resource, CUSTOMER_ID2, "more");
        TestKey key3 = _addEntry(resource, CUSTOMER_ID1, "bar");
        TestKey key4 = _addEntry(resource, CUSTOMER_ID2, "data");
        _addEntry(resource, CUSTOMER_ID3, "abcdef");

        assertEquals(5, entries.getEntryCount());

        // and then see what we can see, for group 1; should see 2 entries first
        FakeHttpResponse response = new FakeHttpResponse();
        FakeHttpRequest request = new FakeHttpRequest()
            .addQueryParam(ClusterMateConstants.QUERY_PARAM_TYPE, ListItemType.ids.toString());
        resource.getHandler().listEntries(request, response, contentKey(CUSTOMER_ID2, ""), null);
        
        if (response.getStatus() != 200) {
            fail("Failed to list (status "+response.getStatus()+"), entity: "+response.getEntity());
        }
        assertEquals(200, response.getStatus());
        assertTrue(response.hasStreamingContent());
        byte[] stuff = this.collectOutput(response);

        ListResponse.IdListResponse idResultList = MAPPER.readValue(stuff, ListResponse.IdListResponse.class);
        assertNotNull(idResultList);
        assertNotNull(idResultList.items);
        assertEquals(2, idResultList.items.size());

        // entries inserted in different order so
        assertEquals(key4, contentKey(idResultList.items.get(0)));
        assertEquals(key2, contentKey(idResultList.items.get(1)));

        // also verify that max entry count is honored
        response = new FakeHttpResponse();
        request = new FakeHttpRequest()
            .addQueryParam(ClusterMateConstants.QUERY_PARAM_TYPE, ListItemType.ids.toString())
            .addQueryParam(ClusterMateConstants.QUERY_PARAM_MAX_ENTRIES, "1");
        resource.getHandler().listEntries(request, response, contentKey(CUSTOMER_ID1, ""), null);
        assertEquals(200, response.getStatus());
        idResultList = MAPPER.readValue(collectOutput(response), ListResponse.IdListResponse.class);

        assertNotNull(idResultList);
        assertNotNull(idResultList.items);
        assertEquals(1, idResultList.items.size());
        assertEquals(key3, contentKey(idResultList.items.get(0)));

        // plus that we can pass "lastSeen" to skip earlier entries
        response = new FakeHttpResponse();
        request = new FakeHttpRequest()
            .addQueryParam(ClusterMateConstants.QUERY_PARAM_TYPE, ListItemType.ids.toString())
            .addQueryParam(ClusterMateConstants.QUERY_PARAM_LAST_SEEN, toBase64(key3));
        resource.getHandler().listEntries(request, response, contentKey(CUSTOMER_ID1, ""), null);
        assertEquals(200, response.getStatus());
        idResultList = MAPPER.readValue(collectOutput(response), ListResponse.IdListResponse.class);

        assertNotNull(idResultList);
        assertNotNull(idResultList.items);
        assertEquals(1, idResultList.items.size());
        assertEquals(key1, contentKey(idResultList.items.get(0)));
        // and, continuing with this
        response = new FakeHttpResponse();
        request = new FakeHttpRequest()
            .addQueryParam(ClusterMateConstants.QUERY_PARAM_TYPE, ListItemType.ids.toString())
            .addQueryParam(ClusterMateConstants.QUERY_PARAM_LAST_SEEN, toBase64(key1));
        resource.getHandler().listEntries(request, response, contentKey(CUSTOMER_ID1, ""), null);
        assertEquals(200, response.getStatus());
        idResultList = MAPPER.readValue(collectOutput(response), ListResponse.IdListResponse.class);

        assertNotNull(idResultList);
        assertNotNull(idResultList.items);
        assertEquals(0, idResultList.items.size());

        // Finally: do sub-trees as well (should use better keys maybe...)
        response = new FakeHttpResponse();
        request = new FakeHttpRequest()
            .addQueryParam(ClusterMateConstants.QUERY_PARAM_TYPE, ListItemType.ids.toString());
        resource.getHandler().listEntries(request, response, contentKey(CUSTOMER_ID1, "fo"), null);
        assertEquals(200, response.getStatus());
        idResultList = MAPPER.readValue(collectOutput(response), ListResponse.IdListResponse.class);
        assertNotNull(idResultList);
        assertNotNull(idResultList.items);
        assertEquals(1, idResultList.items.size());
        assertEquals(key1, contentKey(idResultList.items.get(0)));

        // Let's also verify we can access other list types, to verify basic conversion hooks
        // first name, minimal item
        
        response = new FakeHttpResponse();
        request = new FakeHttpRequest()
            .addQueryParam(ClusterMateConstants.QUERY_PARAM_TYPE, ListItemType.names.toString());
        resource.getHandler().listEntries(request, response, contentKey(CUSTOMER_ID1, "fo"), null);
        assertEquals(200, response.getStatus());

        ListResponse.NameListResponse nameResultList = MAPPER.readValue(collectOutput(response), ListResponse.NameListResponse.class);
        assertNotNull(nameResultList);
        assertNotNull(nameResultList.items);
        assertEquals(1, nameResultList.items.size());
        assertEquals(String.class, nameResultList.items.get(0).getClass());

        response = new FakeHttpResponse();
        request = new FakeHttpRequest()
            .addQueryParam(ClusterMateConstants.QUERY_PARAM_TYPE, ListItemType.minimalEntries.toString());
        resource.getHandler().listEntries(request, response, contentKey(CUSTOMER_ID1, "fo"), null);
        assertEquals(200, response.getStatus());

        ListResponse.MinimalItemListResponse minimalResultList = MAPPER.readValue(collectOutput(response), ListResponse.MinimalItemListResponse.class);
        assertNotNull(minimalResultList);
        assertNotNull(minimalResultList.items);
        assertEquals(1, minimalResultList.items.size());
        assertEquals(ListItem.class, minimalResultList.items.get(0).getClass());

        // and then move on to "full" ListItem
        response = new FakeHttpResponse();
        request = new FakeHttpRequest()
            .addQueryParam(ClusterMateConstants.QUERY_PARAM_TYPE, ListItemType.fullEntries.toString());
        resource.getHandler().listEntries(request, response, contentKey(CUSTOMER_ID1, "fo"), null);
        assertEquals(200, response.getStatus());

        // 07-Feb-2013, tatu: We SHOULD be able to use FullItemListResponse, but, alas, Jackson's
        //   type resolution barfs on it. So instead we must use base ItemListResponse instead...
        JavaType t = MAPPER.constructType(FakeFullListItem.class);
        JavaType fullRespType = MAPPER.getTypeFactory().constructParametricType(ListResponse.class, t);
        byte[] data = collectOutput(response);
        ListResponse<FakeFullListItem> fullResultList = MAPPER.readValue(data, fullRespType);
        assertNotNull(fullResultList);
        assertNotNull(fullResultList.items);
        assertEquals(1, fullResultList.items.size());
        assertEquals(FakeFullListItem.class, fullResultList.items.get(0).getClass());
        
        // clean up:
        resource.getStores().stop();
    }

    private TestKey _addEntry(StoreResourceForTests<TestKey, StoredEntry<TestKey>> resource,
            CustomerId customerId, String path) throws IOException
    {
        final TestKey KEY = contentKey(customerId, path);

        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, KEY);
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response, KEY,
                calcChecksum(SMALL_DATA), new ByteArrayInputStream(SMALL_DATA),
                null, null, null);
        assertEquals(200, response.getStatus());
        return KEY;
    }   
}
