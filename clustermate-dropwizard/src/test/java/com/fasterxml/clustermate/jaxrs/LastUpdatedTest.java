package com.fasterxml.clustermate.jaxrs;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import com.fasterxml.storemate.store.StorableStore;

import com.fasterxml.clustermate.jaxrs.StoreResource;
import com.fasterxml.clustermate.jaxrs.testutil.*;
import com.fasterxml.clustermate.service.ServiceResponse;
import com.fasterxml.clustermate.service.store.StoredEntry;

/**
 * Tests to verify correct handling of 'last updated' timestamps
 * for GETs.
 */
public class LastUpdatedTest extends JaxrsStoreTestBase
{
    // exact id is arbitrary, but use per-test ids for debuggability
    final static CustomerId UNGROUPED = CustomerId.valueOf("OTHR");

    @Override
    public void setUp() {
        initTestLogging();
    }

    // Simple test that creates 3 entries: two under same group, third
    // one as it's "own group"
    public void testLargerFile() throws IOException
    {
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(1000L);
        
        final TestKey KEY1A = contentKey(StoreHandlerForTests.CUSTOMER_WITH_GROUPING, "grouped/key1");
        final TestKey KEY1B = contentKey(StoreHandlerForTests.CUSTOMER_WITH_GROUPING,"grouped/key2");
        final TestKey KEY2 = contentKey(UNGROUPED, "stuff/ungrouped.txt");
        
        // what data we use does not really matter; use diff styles for different compression
        StoreResource<TestKey, StoredEntry<TestKey>> resource = createResource("LastUpdatedTest", timeMaster, true);
        final String DATA1A = biggerCompressibleData(45000);
        final byte[] DATA1A_BYTES = DATA1A.getBytes("UTF-8");
        final String DATA1B = biggerRandomData(1500);
        final byte[] DATA1B_BYTES = DATA1B.getBytes("UTF-8");
        final String DATA2 = biggerSomewhatCompressibleData(16000);
        final byte[] DATA2_BYTES = DATA2.getBytes("UTF-8");

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

        StoredEntry<TestKey> entry2 = rawToEntry(entries.findEntry(KEY2.asStorableKey()));
        assertNotNull(entry2);
        assertEquals(0L, resource.getStores().getLastAccessStore().findLastAccessTime(entry2));
        StoredEntry<TestKey> entry1b = rawToEntry(entries.findEntry(KEY1B.asStorableKey()));
        assertNotNull(entry1b);
        assertEquals(0L, resource.getStores().getLastAccessStore().findLastAccessTime(entry1b));
        StoredEntry<TestKey> entry1a = rawToEntry(entries.findEntry(KEY1A.asStorableKey()));
        assertNotNull(entry1a);
        assertEquals(0L, resource.getStores().getLastAccessStore().findLastAccessTime(entry1a));

        final long UPDATE_TIME1 = 2000L;
        final long UPDATE_TIME2 = 3000L;
        final long UPDATE_TIME3 = 4000L;
        
        // and then try access in a way that updates the timestamp(s)
        timeMaster.setCurrentTimeMillis(UPDATE_TIME1);        
        ServiceResponse resp = resource.getHandler().getEntry(new FakeHttpRequest(),
                new FakeHttpResponse(), KEY2);
        assertNotNull(resp);
        assertFalse(resp.isError());
        assertEquals(UPDATE_TIME1, resource.getStores().getLastAccessStore().findLastAccessTime(entry2));
        
        timeMaster.setCurrentTimeMillis(UPDATE_TIME2);
        resp = resource.getHandler().getEntry(new FakeHttpRequest(), new FakeHttpResponse(), KEY1A);
        assertNotNull(resp);
        assertFalse(resp.isError());
        assertEquals(UPDATE_TIME1, resource.getStores().getLastAccessStore().findLastAccessTime(entry2));
        assertEquals(UPDATE_TIME2, resource.getStores().getLastAccessStore().findLastAccessTime(entry1a));

        // note: second entry should see the last-accessed from the first update!
        entry1b = rawToEntry(entries.findEntry(KEY1B.asStorableKey()));
        assertEquals(FakeLastAccess.GROUPED, entry1b.getLastAccessUpdateMethod());
        assertEquals(UPDATE_TIME2, resource.getStores().getLastAccessStore().findLastAccessTime(entry1b));

        // as well as vice-versa
        timeMaster.setCurrentTimeMillis(UPDATE_TIME3);
        resp = resource.getHandler().getEntry(new FakeHttpRequest(), new FakeHttpResponse(), KEY1B);
        assertEquals(UPDATE_TIME3, resource.getStores().getLastAccessStore().findLastAccessTime(entry1b));
        assertEquals(UPDATE_TIME3, resource.getStores().getLastAccessStore().findLastAccessTime(entry1a));
        entries.stop();
    }
}
