package com.fasterxml.clustermate.jaxrs.common;

import java.io.ByteArrayInputStream;
import java.util.List;

import com.fasterxml.storemate.store.AdminStorableStore;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.StoreOperationSource;

import com.fasterxml.clustermate.jaxrs.StoreResource;
import com.fasterxml.clustermate.jaxrs.testutil.*;
import com.fasterxml.clustermate.service.msg.DeleteResponse;
import com.fasterxml.clustermate.service.msg.PutResponse;
import com.fasterxml.clustermate.service.store.StoredEntry;

/**
 * Unit tests to verify basic functioning of DELETE operation
 */
public abstract class DeleteTestBase extends JaxrsStoreTestBase
{
    final static CustomerId CLIENT_ID = CustomerId.valueOf(123);

    @Override
    public void setUp() {
        initTestLogging();
    }

    protected abstract String testPrefix();
    
    public void testCreateDeleteTwo() throws Exception
    {
        long startTime = 1234L;
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(startTime);

        StoreResource<TestKey, StoredEntry<TestKey>> resource = createResource(testPrefix(), timeMaster, true);
        final String DATA1_STR = "bit of data, very very short";
        final String DATA2_STR = biggerCompressibleData(29000);
        final byte[] DATA1 = DATA1_STR.getBytes("UTF-8");
        final byte[] DATA2 = DATA2_STR.getBytes("UTF-8");

        // ok: assume empty Entity Store
        StorableStore entries = resource.getStores().getEntryStore();
        assertEquals(0, entryCount(entries));

        final String KEY1 = "data/small/1";
        final TestKey INTERNAL_KEY1 = contentKey(CLIENT_ID, KEY1);
        final String KEY2 = "data/bigger-2";
        final TestKey INTERNAL_KEY2 = contentKey(CLIENT_ID, KEY2);

        // first: create 2 entries:
        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                INTERNAL_KEY1, calcChecksum(DATA1), new ByteArrayInputStream(DATA1),
                null, null, null);
        verifyResponseOk(response);
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                INTERNAL_KEY2, calcChecksum(DATA2), new ByteArrayInputStream(DATA2),
                null, null, null);
        verifyResponseOk(response);
        assertEquals(2, entryCount(entries));

        // then verify we can find them:
        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY2);
        verifyResponseOk(response);
        assertTrue(response.hasStreamingContent());

        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        verifyResponseOk(response);
        assertTrue(response.hasStreamingContent());
        // access does update timestamp
        assertEquals(startTime, resource.getStores().getLastAccessStore().findLastAccessTime(
                INTERNAL_KEY1, FakeLastAccess.INDIVIDUAL));

        // then try DELETEing second entry first:
        final long deleteTime = timeMaster.advanceCurrentTimeMillis(5000L).currentTimeMillis();
        
        TestKey deleteKey = contentKey(CLIENT_ID, KEY2);
        resource.getHandler().removeEntry(new FakeHttpRequest(), response, deleteKey);

        verifyResponseOk(response);
        assertSame(DeleteResponse.class, response.getEntity().getClass());
        DeleteResponse<?> dr = (DeleteResponse<?>) response.getEntity();
        assertEquals(deleteKey.toString(), dr.key);
        assertEquals(1, dr.count);

        // count won't change, since there's tombstone:
        assertEquals(2, entryCount(entries));

        // but shouldn't be able to find it any more; 204 indicates this
        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY2);
        // 27-May-2014, tatu: Two possible responses; 204 and 404. Default is 404.
        assertEquals(404, response.getStatus());
        // even though store has the entry
        Storable rawEntry = entries.findEntry(StoreOperationSource.REQUEST, null, INTERNAL_KEY2.asStorableKey());
        assertNotNull(rawEntry);

        assertTrue(rawEntry.isDeleted());

        StoredEntry<TestKey> entry = rawToEntry(rawEntry);

        // important: creationTime does NOT change
        assertEquals(startTime, entry.getCreationTime());
        // but last-modified should
        assertEquals(Long.toHexString(deleteTime), Long.toHexString(entry.getLastModifiedTime()));

        // but the other entry is there
        entry = rawToEntry(entries.findEntry(StoreOperationSource.REQUEST, null, INTERNAL_KEY1.asStorableKey()));
        assertNotNull(entry);
        assertFalse(entry.isDeleted());
        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        verifyResponseOk(response);

        // But content dump should give two non-deleted entries...
        List<Storable> e = ((AdminStorableStore) entries).dumpEntries(StoreOperationSource.ADMIN_TOOL, 10, true);
        assertEquals(2, e.size());

        // ok to "re-delete"...
        response = new FakeHttpResponse();
        resource.getHandler().removeEntry(new FakeHttpRequest(), response,
                contentKey(CLIENT_ID, KEY2));
        verifyResponseOk(response);
        assertSame(DeleteResponse.class, response.getEntity().getClass());

        // then, delete the other entry as well
        response = new FakeHttpResponse();
        deleteKey = contentKey(CLIENT_ID, KEY1);
        resource.getHandler().removeEntry(new FakeHttpRequest(), response,
                deleteKey);
        verifyResponseOk(response);
        assertSame(DeleteResponse.class, response.getEntity().getClass());
        dr = (DeleteResponse<?>) response.getEntity();
        assertEquals(deleteKey.toString(), dr.key);

        assertEquals(2, entryCount(entries));
        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        // 27-May-2014, tatu: Two possible responses; 204 and 404. Default is 404.
        assertEquals(404, response.getStatus());

        // clean up:
        resource.getStores().stop();
    }
    
    /**
     * Test that verifies that an attempt to re-create a deleted resource
     * fails, unless specifically instructed to be allowed
     */
    public void testDeleteTryRecreateFail() throws Exception
    {
        long startTime = 1234L;
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(startTime);
        StoreResource<TestKey, StoredEntry<TestKey>> resource = createResource(testPrefix()+"RecreateFAIL", timeMaster, true);
        final byte[] DATA1 = "bit of data".getBytes("UTF-8");

        StorableStore entries = resource.getStores().getEntryStore();
        assertEquals(0, entryCount(entries));

        final String KEY1 = "data/small/1";
        final TestKey INTERNAL_KEY1 = contentKey(CLIENT_ID, KEY1);

        // first: create the entry
        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                INTERNAL_KEY1, calcChecksum(DATA1), new ByteArrayInputStream(DATA1),
                null, null, null);
        verifyResponseOk(response);
        assertEquals(1, entryCount(entries));

        // then DELETE it
        timeMaster.advanceCurrentTimeMillis(10L);
        resource.getHandler().removeEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        verifyResponseOk(response);
        assertSame(DeleteResponse.class, response.getEntity().getClass());
        DeleteResponse<?> dr = (DeleteResponse<?>) response.getEntity();
        assertEquals(INTERNAL_KEY1.toString(), dr.key);
        // count won't change, since there's tombstone:
        assertEquals(1, entryCount(entries));

        // but then an attempt to "re-PUT" entry must fail with 410 (not conflict)
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                INTERNAL_KEY1, calcChecksum(DATA1), new ByteArrayInputStream(DATA1),
                null, null, null);
        assertEquals(410, response.getStatus());
        PutResponse<?> pr = (PutResponse<?>) response.getEntity();
        verifyMessage("Failed PUT: trying to recreate", pr.message);

        // and also should not be visible any more
        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        // 27-May-2014, tatu: Two possible responses; 204 and 404. Default is 404.
        assertEquals(404, response.getStatus());
    }

    /**
     * Test that verifies that an attempt to re-create a deleted resource
     * success iff explicitly allowed.
     */
    public void testDeleteTryRecreateOK() throws Exception
    {
        long startTime = 1234L;
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(startTime);
        StoreResource<TestKey, StoredEntry<TestKey>> resource = createResource(testPrefix()+"RecreateOK", timeMaster, true);
        // IMPORTANT: need to explicitly enable
        resource.getServiceConfig().cfgAllowUndelete = true;
        final String PAYLOAD = "tiny gob of stuff, ends up inlined";
        final byte[] DATA1 = PAYLOAD.getBytes("UTF-8");

        StorableStore entries = resource.getStores().getEntryStore();
        assertEquals(0, entryCount(entries));

        final String KEY1 = "data/small/1";
        final TestKey INTERNAL_KEY1 = contentKey(CLIENT_ID, KEY1);

        // PUT, DELETE
        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                INTERNAL_KEY1, calcChecksum(DATA1), new ByteArrayInputStream(DATA1),
                null, null, null);
        verifyResponseOk(response);
        assertEquals(1, entryCount(entries));

        timeMaster.advanceCurrentTimeMillis(10L);
        resource.getHandler().removeEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        verifyResponseOk(response);
        assertSame(DeleteResponse.class, response.getEntity().getClass());
        DeleteResponse<?> dr = (DeleteResponse<?>) response.getEntity();
        assertEquals(INTERNAL_KEY1.toString(), dr.key);
        // count won't change, since there's tombstone:
        assertEquals(1, entryCount(entries));

        // and this ought to be fine too
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                INTERNAL_KEY1, calcChecksum(DATA1), new ByteArrayInputStream(DATA1),
                null, null, null);
        verifyResponseOk(response);
        PutResponse<?> pr = (PutResponse<?>) response.getEntity();
        assertNotNull(pr);

        // and make entry accessible
        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        verifyResponseOk(response);

        assertTrue(response.hasInlinedData());
        byte[] data = collectOutput(response);
        assertEquals(PAYLOAD, new String(data, "UTF-8"));

        // Oh. One more try; now with different data, ought to fail. So, DELETE again:
        resource.getHandler().removeEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        verifyResponseOk(response);
        // and then try PUT with bit different data...

        response = new FakeHttpResponse();
        final byte[] DATA2 = "Foobar!".getBytes("UTF-8");
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                INTERNAL_KEY1, calcChecksum(DATA2), new ByteArrayInputStream(DATA2),
                null, null, null);
        assertEquals(409, response.getStatus());
    }

    public void testMultiDelete() throws Exception
    {
        long startTime = 1234L;
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(startTime);
        StoreResource<TestKey, StoredEntry<TestKey>> resource = createResource(testPrefix()+"MultiDelete", timeMaster, true);
        final String PAYLOAD = "some stuff, inlined, whatever";
        final byte[] DATA1 = PAYLOAD.getBytes("UTF-8");

        StorableStore entries = resource.getStores().getEntryStore();
        assertEquals(0, entryCount(entries));

        final TestKey INTERNAL_KEY0 = contentKey(CLIENT_ID, "data/basic/0");
        final TestKey INTERNAL_KEY1 = contentKey(CLIENT_ID, "data/small/1");
        final TestKey INTERNAL_KEY2 = contentKey(CLIENT_ID, "data/small/2");
        final TestKey INTERNAL_KEY3 = contentKey(CLIENT_ID, "data/tiny/1");

        FakeHttpResponse response;
        // PUT all first
        for (TestKey key : new TestKey[] {
                INTERNAL_KEY0, INTERNAL_KEY1, INTERNAL_KEY2, INTERNAL_KEY3    
        }) {
            response = new FakeHttpResponse();
            resource.getHandler().putEntry(new FakeHttpRequest(), response,
                    key, calcChecksum(DATA1), new ByteArrayInputStream(DATA1),
                    null, null, null);
            verifyResponseOk(response);
        }
        assertEquals(4, entryCount(entries));

        timeMaster.advanceCurrentTimeMillis(10L);

        final TestKey PREFIX = contentKey(CLIENT_ID, "data/small/");
        response = new FakeHttpResponse();
        resource.getHandler().removeEntries(new FakeHttpRequest(), response, PREFIX, null);

        verifyResponseOk(response);
        
        assertSame(DeleteResponse.class, response.getEntity().getClass());
        DeleteResponse<?> dr = (DeleteResponse<?>) response.getEntity();

        // should have deleted 2 entries
        assertEquals(2, dr.count);
        assertTrue(dr.complete);
        assertEquals(PREFIX.toString(), dr.key);

        // count won't change, since there are tombstones left
        assertEquals(4, entryCount(entries));

        for (TestKey key : new TestKey[] { INTERNAL_KEY1, INTERNAL_KEY2 }) {
            Storable rawEntry = entries.findEntry(StoreOperationSource.REQUEST, null, key.asStorableKey());
            assertNotNull(rawEntry);
            assertTrue(rawEntry.isDeleted());
        }

        // but other ones remain
        for (TestKey key : new TestKey[] { INTERNAL_KEY0, INTERNAL_KEY3 }) {
            Storable rawEntry = entries.findEntry(StoreOperationSource.REQUEST, null, key.asStorableKey());
            assertNotNull(rawEntry);
            assertFalse(rawEntry.isDeleted());
        }
    }
}
