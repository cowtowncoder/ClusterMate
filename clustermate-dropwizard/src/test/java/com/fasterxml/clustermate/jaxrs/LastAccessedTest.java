package com.fasterxml.clustermate.jaxrs;

import java.io.ByteArrayInputStream;
import java.util.*;

import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.StoreException;
import com.fasterxml.storemate.store.StoreOperationSource;
import com.fasterxml.storemate.store.backend.IterationAction;

import com.fasterxml.clustermate.jaxrs.StoreResource;
import com.fasterxml.clustermate.jaxrs.testutil.*;
import com.fasterxml.clustermate.service.LastAccessStore;
import com.fasterxml.clustermate.service.LastAccessStore.LastAccessIterationCallback;
import com.fasterxml.clustermate.service.ServiceResponse;
import com.fasterxml.clustermate.service.store.EntryLastAccessed;
import com.fasterxml.clustermate.service.store.StoredEntry;

/**
 * Tests to verify correct handling of 'last updated' timestamps
 * for GETs.
 */
public class LastAccessedTest extends JaxrsStoreTestBase
{
    // exact id is arbitrary, but use per-test ids for debuggability
    final static CustomerId UNGROUPED = CustomerId.valueOf("OTHR");

    @Override
    public void setUp() {
        initTestLogging();
    }

    // Simple test that creates 3 entries: two under same group, third
    // one as it's "own group"
    public void testGroupedLastAccess() throws Exception
    {
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(1000L);

        final TestKey KEY1A = contentKey(StoreHandlerForTests.CUSTOMER_WITH_GROUPING, "grouped/key1");
        final TestKey KEY1B = contentKey(StoreHandlerForTests.CUSTOMER_WITH_GROUPING, "grouped/key2");
        final TestKey KEY2 = contentKey(UNGROUPED, "stuff/ungrouped.txt");
        
        // what data we use does not really matter; use diff styles for different compression
        StoreResource<TestKey, StoredEntry<TestKey>> resource = createResource("LastUpdatedTest", timeMaster, true);
        final String DATA1A = biggerCompressibleData(45000);
        final byte[] DATA1A_BYTES = DATA1A.getBytes("UTF-8");
        final String DATA1B = biggerRandomData(1500);
        final byte[] DATA1B_BYTES = DATA1B.getBytes("UTF-8");
        final String DATA2 = biggerSomewhatCompressibleData(16000);
        final byte[] DATA2_BYTES = DATA2.getBytes("UTF-8");

        final LastAccessStore<TestKey, StoredEntry<TestKey>> accessStore = resource.getStores().getLastAccessStore();

        // verify that first one doesn't exist initially (sanity check)
        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, KEY1A);
        assertEquals(404, response.getStatus());

        // then try adding entries
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                KEY1A, calcChecksum(DATA1A_BYTES), new ByteArrayInputStream(DATA1A_BYTES),
                null, null, null);
        assertEquals(200, response.getStatus());
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                KEY2, calcChecksum(DATA2_BYTES), new ByteArrayInputStream(DATA2_BYTES),
                null, null, null);
        assertEquals(200, response.getStatus());
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                KEY1B, calcChecksum(DATA1B_BYTES), new ByteArrayInputStream(DATA1B_BYTES),
                null, null, null);
        assertEquals(200, response.getStatus());

        // find entries; should not yet have last-accessed timestamps
        final StorableStore entries = resource.getStores().getEntryStore();
        assertEquals(3, entries.getEntryCount());

        StoredEntry<TestKey> entry2 = rawToEntry(entries.findEntry(StoreOperationSource.REQUEST, KEY2.asStorableKey()));
        assertNotNull(entry2);
        assertEquals(0L, accessStore.findLastAccessTime(entry2));
        StoredEntry<TestKey> entry1b = rawToEntry(entries.findEntry(StoreOperationSource.REQUEST, KEY1B.asStorableKey()));
        assertNotNull(entry1b);
        assertEquals(0L, accessStore.findLastAccessTime(entry1b));
        StoredEntry<TestKey> entry1a = rawToEntry(entries.findEntry(StoreOperationSource.REQUEST, KEY1A.asStorableKey()));
        assertNotNull(entry1a);
        assertEquals(0L, accessStore.findLastAccessTime(entry1a));

        final long UPDATE_TIME1 = 2000L;
        final long UPDATE_TIME2 = 3000L;
        final long UPDATE_TIME3 = 4000L;
        
        // and then try access in a way that updates the timestamp(s)
        timeMaster.setCurrentTimeMillis(UPDATE_TIME1);        
        ServiceResponse resp = resource.getHandler().getEntry(new FakeHttpRequest(),
                new FakeHttpResponse(), KEY2);
        assertNotNull(resp);
        assertFalse(resp.isError());
        assertEquals(UPDATE_TIME1, accessStore.findLastAccessTime(entry2));
        
        timeMaster.setCurrentTimeMillis(UPDATE_TIME2);
        resp = resource.getHandler().getEntry(new FakeHttpRequest(), new FakeHttpResponse(), KEY1A);
        assertNotNull(resp);
        assertFalse(resp.isError());
        assertEquals(UPDATE_TIME1, accessStore.findLastAccessTime(entry2));
        assertEquals(UPDATE_TIME2, accessStore.findLastAccessTime(entry1a));

        // note: second entry should see the last-accessed from the first update!
        entry1b = rawToEntry(entries.findEntry(StoreOperationSource.REQUEST, KEY1B.asStorableKey()));
        assertEquals(FakeLastAccess.GROUPED, entry1b.getLastAccessUpdateMethod());
        assertEquals(UPDATE_TIME2, accessStore.findLastAccessTime(entry1b));

        // as well as vice-versa
        timeMaster.setCurrentTimeMillis(UPDATE_TIME3);
        resp = resource.getHandler().getEntry(new FakeHttpRequest(), new FakeHttpResponse(), KEY1B);
        assertEquals(UPDATE_TIME3, accessStore.findLastAccessTime(entry1b));
        assertEquals(UPDATE_TIME3, accessStore.findLastAccessTime(entry1a));

        assertEquals(2L, accessStore.getEntryCount());

        // Also: we should be able to iterate over last-access entry...
        final Map<TestKey,EntryLastAccessed> map = new HashMap<TestKey,EntryLastAccessed>();
        accessStore.scanEntries(new LastAccessIterationCallback() {
            @Override
            public IterationAction processEntry(StorableKey key,
                    EntryLastAccessed entry) throws StoreException {
                map.put(contentKey(key), entry);
                return IterationAction.PROCESS_ENTRY;
            }
        });
        assertEquals(2, map.size());

        // and they need to match, too; KEY2 fine as is, but need group key:
        final TestKey GROUP_KEY = contentKey(StoreHandlerForTests.CUSTOMER_WITH_GROUPING, "");

        EntryLastAccessed grouped = map.get(GROUP_KEY);
        assertNotNull(grouped);
        assertEquals(UPDATE_TIME3, grouped.lastAccessTime);
        // use explicit formula instead of StoredEntry.calculateMaxExpirationTime()
        assertEquals(entry1a.getCreationTime() + 1000 * entry1a.getMaxTTLSecs(), grouped.expirationTime);
        
        EntryLastAccessed ungrouped = map.get(KEY2);
        assertNotNull(ungrouped);
        assertEquals(UPDATE_TIME1, ungrouped.lastAccessTime);
        assertEquals(entry2.getCreationTime() + 1000 * entry2.getMaxTTLSecs(), ungrouped.expirationTime);

        // One more thing: deletions. Individual entries -> last-access should disappear
        response = new FakeHttpResponse();
        resource.getHandler().removeEntry(new FakeHttpRequest(), response, KEY2);
        assertEquals(200, response.getStatus());

        assertEquals(0L, accessStore.findLastAccessTime(entry2));
        assertEquals(UPDATE_TIME3, accessStore.findLastAccessTime(entry1b));
        assertEquals(UPDATE_TIME3, accessStore.findLastAccessTime(entry1a));

        // and grouped; no deletion.

        response = new FakeHttpResponse();
        resource.getHandler().removeEntry(new FakeHttpRequest(), response, KEY1A);
        assertEquals(200, response.getStatus());

        assertEquals(0L, accessStore.findLastAccessTime(entry2));
        assertEquals(UPDATE_TIME3, accessStore.findLastAccessTime(entry1b));
        assertEquals(UPDATE_TIME3, accessStore.findLastAccessTime(entry1a));

        // Close'em
        entries.stop();
        accessStore.stop();
    }
}
