package com.fasterxml.clustermate.jaxrs.common;

import java.io.ByteArrayInputStream;

import org.junit.Assert;

import com.fasterxml.clustermate.jaxrs.StoreResource;
import com.fasterxml.clustermate.jaxrs.testutil.*;
import com.fasterxml.clustermate.service.msg.PutResponse;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.StoreOperationSource;

public abstract class MediumFileTestBase extends JaxrsStoreTestBase
{
    final static CustomerId CLIENT_ID = CustomerId.valueOf("MED_");

    @Override
    public void setUp() {
        initTestLogging();
    }

    protected abstract String testPrefix();
    
    // Test to use GZIP
    public void testMediumFile() throws Exception
    {
        final long startTime = 1234L;
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(startTime);
		
        StoreResource<TestKey, StoredEntry<TestKey>> resource = createResource(testPrefix(), timeMaster, true);
        // ensure it'll use gzip compression
        int origSize = new ServiceConfigForTests().storeConfig.maxUncompressedSizeForGZIP - 100;
        final String BIG_STRING = biggerCompressibleData(origSize);
        final byte[] BIG_DATA = BIG_STRING.getBytes("UTF-8");
        
        // ok: assume empty Entity Store
        StorableStore entries = resource.getStores().getEntryStore();
        assertEquals(0, entryCount(entries));
        
        // then try to find bogus entry; make sure to use a slash...
        final TestKey INTERNAL_KEY1 = contentKey(CLIENT_ID,  "data/medium/1");

        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        assertEquals(404, response.getStatus());

        // then try adding said entry
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                INTERNAL_KEY1, calcChecksum(BIG_DATA), new ByteArrayInputStream(BIG_DATA),
                null, null, null);
        assertEquals(200, response.getStatus());
        // verify we got a file...
        assertSame(PutResponse.class, response.getEntity().getClass());
        PutResponse<?> presp = (PutResponse<?>) response.getEntity();

        assertFalse(presp.inlined);

        // can we count on this getting updated? Seems to be, FWIW
        assertEquals(1, entryCount(entries));

        // Ok. Then, we should also be able to fetch it, right?
        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        assertEquals(200, response.getStatus());

        // big enough, should be backed by a real file...
        assertTrue(response.hasStreamingContent());
        assertTrue(response.hasFile());
        byte[] data = collectOutput(response);
        assertEquals(BIG_DATA.length, data.length);
        Assert.assertArrayEquals(BIG_DATA, data);

        // and more fundamentally, verify store had it:
        StoredEntry<TestKey> entry = rawToEntry(entries.findEntry(StoreOperationSource.REQUEST,
                null, INTERNAL_KEY1.asStorableKey()));
        assertNotNull(entry);
        assertTrue(entry.hasExternalData());
        assertFalse(entry.hasInlineData());
        assertEquals(Compression.GZIP, entry.getCompression());
        assertEquals(BIG_DATA.length, entry.getActualUncompressedLength());
        // compressed, should be smaller...
        assertTrue("Should be compressed; size="+entry.getActualUncompressedLength()+"; storage-size="+entry.getStorageLength(),
                entry.getActualUncompressedLength() > entry.getStorageLength());
        assertEquals(startTime, entry.getCreationTime());
        assertEquals(startTime, entry.getLastModifiedTime());
        assertEquals(startTime, resource.getStores().getLastAccessStore().findLastAccessTime(entry));

        entries.stop();
    }

    // Different test, to trigger different code path
    public void testMediumUncompressible() throws Exception
    {
        final long startTime = 5000L;
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(startTime);
          
        StoreResource<TestKey, StoredEntry<TestKey>> resource = createResource(testPrefix()+"-noncomp", timeMaster, true);

        int origSize = new ServiceConfigForTests().storeConfig.maxUncompressedSizeForGZIP + 7777;
        final String BIG_STRING = biggerRandomData(origSize);
        final byte[] BIG_DATA0 = BIG_STRING.getBytes("UTF-8");
        // Hmmh. Still somewhat compressible; so let's LZF compress for fun on client side
        final byte[] BIG_DATA = Compressors.lzfCompress(BIG_DATA0);

        StorableStore entries = resource.getStores().getEntryStore();
        assertEquals(0, entryCount(entries));
        
        final TestKey INTERNAL_KEY1 = contentKey(CLIENT_ID,  "data/medium/2");

        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                INTERNAL_KEY1, calcChecksum(BIG_DATA), new ByteArrayInputStream(BIG_DATA),
                null, null, null);
        assertEquals(200, response.getStatus());
        // verify we got a file...
        assertSame(PutResponse.class, response.getEntity().getClass());
        PutResponse<?> presp = (PutResponse<?>) response.getEntity();

        assertFalse(presp.inlined);

        assertEquals(1, entryCount(entries));

        // Ok. Then, we should also be able to fetch it, right?
        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        assertEquals(200, response.getStatus());

        // big enough, should be backed by a real file...
        assertTrue(response.hasStreamingContent());
        assertTrue(response.hasFile());
        byte[] data = collectOutput(response);
        assertEquals(BIG_DATA.length, data.length);
        Assert.assertArrayEquals(BIG_DATA, data);

        // and more fundamentally, verify store had it:
        StoredEntry<TestKey> entry = rawToEntry(entries.findEntry(StoreOperationSource.REQUEST,
                null, INTERNAL_KEY1.asStorableKey()));
        assertNotNull(entry);
        assertTrue(entry.hasExternalData());
        assertFalse(entry.hasInlineData());
        assertEquals(Compression.NONE, entry.getCompression());
        assertEquals(BIG_DATA.length, entry.getActualUncompressedLength());
        assertEquals(startTime, entry.getCreationTime());
        assertEquals(startTime, entry.getLastModifiedTime());
        assertEquals(startTime, resource.getStores().getLastAccessStore().findLastAccessTime(entry));

        entries.stop();
    }

}
