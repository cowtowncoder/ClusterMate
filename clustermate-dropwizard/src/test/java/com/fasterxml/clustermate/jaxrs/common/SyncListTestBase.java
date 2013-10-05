package com.fasterxml.clustermate.jaxrs.common;

import java.io.ByteArrayInputStream;

import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.util.OperationDiagnostics;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.ContentType;
import com.fasterxml.clustermate.api.KeyRange;
import com.fasterxml.clustermate.jaxrs.testutil.*;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.sync.SyncHandler;
import com.fasterxml.clustermate.service.sync.SyncListResponse;

/**
 * Test case(s) to verify that we can handle basic pull list request
 */
public abstract class SyncListTestBase extends JaxrsStoreTestBase
{
    final static CustomerId CLIENT_ID = CustomerId.valueOf(1234);

    /**
     * NOTE: should determine from server settings for "cfgSyncGracePeriod", but
     * for now can be inlined.
     */
    final long GRACE_PERIOD = 5000L;

    @Override
    public void setUp() {
        initTestLogging();
    }

    public void testSimpleSyncList() throws Exception
    {
        final long creationTime = 1234L;
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(creationTime);

        StoreResourceForTests<TestKey, StoredEntry<TestKey>> resource = createResource("syncSimple", timeMaster, true);

        SyncHandler<TestKey, StoredEntry<TestKey>> syncH = new SyncHandler<TestKey, StoredEntry<TestKey>>(resource.getStuff(),
                resource.getStores(), resource.getCluster());

        final long SYNC_GRACE_PERIOD_MSECS = syncH.getSyncGracePeriodMsecs();
        // First, add an entry:
        StorableStore entries = resource.getStores().getEntryStore();
        final TestKey KEY1 = contentKey(CLIENT_ID, "data/entry/1");
        final byte[] SMALL_DATA = "Some data that we want to store -- small, gets inlined...".getBytes("UTF-8");

        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, KEY1);
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response, KEY1,
                calcChecksum(SMALL_DATA), new ByteArrayInputStream(SMALL_DATA),
                null, null, null);
        assertEquals(200, response.getStatus());
        assertEquals(1, entryCount(entries));

        SyncListResponse<?> syncList;

        // then verify that that entry is NOT visible:
        OperationDiagnostics diag = new OperationDiagnostics(0L);        

        syncList = _fetchSyncList(resource, syncH, creationTime, diag);

        assertEquals(0, diag.getItemCount());
        assertNotNull(syncList);
        assertNull(syncList.message);
        assertNotNull(syncList.entries);
        assertEquals(0, syncList.entries.size());
 
        // Once more, by mild advance
        timeMaster.advanceCurrentTimeMillis(SYNC_GRACE_PERIOD_MSECS / 4);
        diag = new OperationDiagnostics(0L);
        syncList = _fetchSyncList(resource, syncH, creationTime, diag);

        assertEquals(0, diag.getItemCount());
        assertNotNull(syncList);
        assertNull(syncList.message);
        assertNotNull(syncList.entries);
        assertEquals(0, syncList.entries.size());
        
        // then advance time, try to access change list
        timeMaster.advanceCurrentTimeMillis(SYNC_GRACE_PERIOD_MSECS);
        long callTime = timeMaster.currentTimeMillis();

        diag = new OperationDiagnostics(0L);
        syncList = _fetchSyncList(resource, syncH, creationTime, diag);

        assertEquals(1, diag.getItemCount());
        assertNotNull(syncList);
        assertNull(syncList.message);
        assertNotNull(syncList.entries);
        assertEquals(1, syncList.entries.size());

        // Last actual timestamp would be 1234L; but we should get "currentTime - gracePeriod" here
        // NOTE: will break if config defaults change. Should be improved somehow.
        assertEquals(callTime - SYNC_GRACE_PERIOD_MSECS , syncList.lastSeen());

        // clean up:
        resource.getStores().stop();
    }

    private SyncListResponse<StoredEntry<?>> _fetchSyncList(StoreResourceForTests<TestKey, StoredEntry<TestKey>> resource,
            SyncHandler<TestKey, StoredEntry<TestKey>> syncH,
            long creationTime,
            OperationDiagnostics diag) throws Exception
    {
        final KeyRange localRange = resource.getKeyRange();
        FakeHttpRequest syncReq = new FakeHttpRequest();
        syncReq.addQueryParam(ClusterMateConstants.QUERY_PARAM_KEYRANGE_START, ""+localRange.getStart());
        syncReq.addQueryParam(ClusterMateConstants.QUERY_PARAM_KEYRANGE_LENGTH, ""+localRange.getLength());
        // JSON or Smile? Either should be fine...
        syncReq.addHeader(ClusterMateConstants.HTTP_HEADER_ACCEPT, ContentType.SMILE.toString());
        
        FakeHttpResponse response = new FakeHttpResponse();
        
        syncH.listEntries(syncReq, response, creationTime, diag);
        assertTrue(response.hasStreamingContent());
        assertEquals(200, response.getStatus());
        assertEquals(ContentType.SMILE.toString(), response.getContentType());
        byte[] data = response.getStreamingContentAsBytes();

        return resource.getStuff().smileReader(SyncListResponse.class).readValue(data);
    }

    /**
     * Test to verify that in case of large block of entries with same last-mod
     * timestamp, max entries to list must be relaxed so that caller
     * can advance.
     */
    public void testLargerSyncList() throws Exception
    {
        final long creationTime = 1234L;
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(creationTime);

        StoreResourceForTests<TestKey, StoredEntry<TestKey>> resource = createResource("syncLarger", timeMaster, true);

        // set handler's "max-to-list" to 1, less than what we need later on
        SyncHandler<TestKey, StoredEntry<TestKey>> syncH = new SyncHandler<TestKey, StoredEntry<TestKey>>(resource.getStuff(),
                resource.getStores(), resource.getCluster(), 1);
        
        // First, add entries
        StorableStore entries = resource.getStores().getEntryStore();
        final TestKey KEY1 = contentKey(CLIENT_ID, "data/entry/1");
        final TestKey KEY2 = contentKey(CLIENT_ID, "data/entry/2");
        final TestKey KEY3 = contentKey(CLIENT_ID, "data/entry/3");
        final byte[] SMALL_DATA = "Some data that we want to store -- small, gets inlined...".getBytes("UTF-8");
        final int hash = calcChecksum(SMALL_DATA);

        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response, KEY1,
                hash, new ByteArrayInputStream(SMALL_DATA),
                null, null, null);
        assertEquals(200, response.getStatus());
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response, KEY2,
                hash, new ByteArrayInputStream(SMALL_DATA),
                null, null, null);
        assertEquals(200, response.getStatus());
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response, KEY3,
                hash, new ByteArrayInputStream(SMALL_DATA),
                null, null, null);
        assertEquals(200, response.getStatus());
        
        assertEquals(3, entryCount(entries));
        timeMaster.advanceCurrentTimeMillis(30000L);

        FakeHttpRequest syncReq = new FakeHttpRequest();
        final KeyRange localRange = resource.getKeyRange();
        syncReq.addQueryParam(ClusterMateConstants.QUERY_PARAM_KEYRANGE_START, ""+localRange.getStart());
        syncReq.addQueryParam(ClusterMateConstants.QUERY_PARAM_KEYRANGE_LENGTH, ""+localRange.getLength());
        syncReq.addHeader(ClusterMateConstants.HTTP_HEADER_ACCEPT, ContentType.SMILE.toString());
        
        final long callTime = timeMaster.currentTimeMillis();
        OperationDiagnostics diag = new OperationDiagnostics(0L);        
        SyncListResponse<StoredEntry<?>> syncList = _fetchSyncList(resource, syncH, creationTime, diag);
        assertNotNull(syncList);
        assertNull(syncList.message);
        assertNotNull(syncList.entries);
        // Important: we MUST get all 3, as they have same timestamp
        assertEquals(3, syncList.entries.size());
        assertEquals(3, diag.getItemCount());
        
        assertEquals(callTime - GRACE_PERIOD, syncList.lastSeen());
    }
}
