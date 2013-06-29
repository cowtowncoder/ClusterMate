package com.fasterxml.clustermate.jaxrs;

import java.io.*;

import org.junit.Assert;

import com.fasterxml.clustermate.jaxrs.StoreResource;
import com.fasterxml.clustermate.jaxrs.testutil.*;
import com.fasterxml.clustermate.service.msg.PutResponse;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.StoreConfig;
import com.fasterxml.storemate.store.StoreOperationSource;

public class LargeFileTest extends JaxrsStoreTestBase
{
    final static int MAX_PAYLOAD_IN_MEMORY = StoreConfig.DEFAULT_MIN_PAYLOAD_FOR_STREAMING-1;

    final static CustomerId CLIENT_ID = CustomerId.valueOf("BIG_");

    @Override
    public void setUp() {
        initTestLogging();
    }
    
    // Test larger content that should end up as file, with LZF encoding
    // if client sends it uncompressed.
    public void testLargerEntry100Kilos() throws Exception
    {
        _testLargerEntry(MAX_PAYLOAD_IN_MEMORY + 100);
    }
    
    public void testLargerEntry3Megs() throws Exception
    {
        _testLargerEntry(3 * 1024 * 1024);
    }
        
    protected void _testLargerEntry(int origSize) throws Exception
    {
        final long startTime = 1234L;
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(startTime);
	
        // Need to make big enough to use streaming, too...
        StoreResource<TestKey, StoredEntry<TestKey>> resource = createResource("large"+origSize,
                timeMaster, true);
        final String BIG_STRING = biggerCompressibleData(origSize);
        final byte[] BIG_DATA = BIG_STRING.getBytes("UTF-8");

        // ok: assume empty Entity Store
        StorableStore entries = resource.getStores().getEntryStore();
        assertEquals(0, entries.getEntryCount());

        // then try to find bogus entry; make sure to use a slash...
        final TestKey INTERNAL_KEY1 = contentKey(CLIENT_ID, "data/big/1");

        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        assertEquals(404, response.getStatus());

        FakeHttpRequest request = new FakeHttpRequest();

        // then try adding said entry
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(request, response,
                INTERNAL_KEY1, calcChecksum(BIG_DATA), new ByteArrayInputStream(BIG_DATA),
                null, null, null);
        assertEquals(200, response.getStatus());
        // verify we got a file...
        assertSame(PutResponse.class, response.getEntity().getClass());
        PutResponse<?> presp = (PutResponse<?>) response.getEntity();
        assertEquals(Compression.LZF, presp.compression);
        assertFalse(presp.inlined);

        // can we count on this getting updated? Seems to be, FWIW
        assertEquals(1, entries.getEntryCount());

        // Ok. Then, we should also be able to fetch it, right?
        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        assertEquals(200, response.getStatus());

        assertTrue(response.hasStreamingContent());
        // big enough, should be backed by a real file...
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
        String path = entry.getRaw().getExternalFilePath();

        // bit fragile but:
        if (!path.startsWith("1970-01-01/00:00/")) {
            fail("Odd path: "+path);
        }
        String last = path.substring(path.lastIndexOf('/')+1);
        assertEquals("0000:BIG_data_big_1.L", last);

        assertEquals(Compression.LZF, entry.getCompression());
        assertEquals(BIG_DATA.length, entry.getActualUncompressedLength());
        // compressed, should be smaller...
        assertTrue("Should be compressed; size="+entry.getActualUncompressedLength()+"; storage-size="+entry.getStorageLength(),
                entry.getActualUncompressedLength() > entry.getStorageLength());
        assertEquals(startTime, entry.getCreationTime());
        assertEquals(startTime, entry.getLastModifiedTime());
        assertEquals(startTime, resource.getStores().getLastAccessStore().findLastAccessTime(entry));

        entries.stop();
    }
    
    public void testLargerGZIPEntry() throws Exception
    {
        final long startTime = 1234L;
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(startTime);
        
        // Need to make big enough to use streaming, but should be compressible as well
        int origSize = 256000;
        StoreResource<TestKey, StoredEntry<TestKey>> resource = createResource("largeGzip", timeMaster, true);
        final String BIG_STRING = biggerSomewhatCompressibleData(origSize);
        final byte[] BIG_DATA_ORIG = BIG_STRING.getBytes("UTF-8");
        final byte[] BIG_DATA_GZIP = gzip(BIG_DATA_ORIG);
        
        // ok: assume empty Entity Store
        StorableStore entries = resource.getStores().getEntryStore();
        assertEquals(0, entries.getEntryCount());

        final TestKey INTERNAL_KEY1 = contentKey(CLIENT_ID, "data/bigZ-1");

        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        assertEquals(404, response.getStatus());

        // then try adding said entry, and letting server know it's gzip'd
        FakeHttpRequest request = new FakeHttpRequest()
            .addHeader("Content-Encoding", "gzip");
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(request, response,
                INTERNAL_KEY1, calcChecksum(BIG_DATA_GZIP), new ByteArrayInputStream(BIG_DATA_GZIP),
                null, null, null);

        // verify we got a file, and that its size is the same as what we passed
        assertSame(PutResponse.class, response.getEntity().getClass());
        PutResponse<?> presp = (PutResponse<?>) response.getEntity();
        if (response.getStatus() != 200) {
            fail("Expected 200 response, got "+response.getStatus()+"; message: "+presp.message);
        }

        assertFalse(presp.inlined);
        assertEquals(BIG_DATA_GZIP.length, presp.storageSize);
        assertEquals(-1, presp.size);

        // can we count on this getting updated? Seems to be, FWIW
        assertEquals(1, entries.getEntryCount());

        // Ok. Then, we should also be able to fetch it, right?
        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        assertEquals(200, response.getStatus());
        assertTrue(response.hasStreamingContent());
        // big enough, should be backed by a real file.
        assertTrue(response.hasFile());
        byte[] data = collectOutput(response);
        // also: unless we say it's ok, should get properly uncompressed content:
        assertEquals(BIG_DATA_ORIG.length, data.length);
        Assert.assertArrayEquals(BIG_DATA_ORIG, data);

        StoredEntry<TestKey> entry = rawToEntry(entries.findEntry(StoreOperationSource.REQUEST,
                null, INTERNAL_KEY1.asStorableKey()));
        assertNotNull(entry);
        assertTrue(entry.hasExternalData());
        assertFalse(entry.hasInlineData());
        assertEquals(Compression.GZIP, entry.getCompression());
        assertEquals(BIG_DATA_GZIP.length, entry.getStorageLength());
        assertEquals(-1, entry.getRaw().getOriginalLength());
        assertEquals(startTime, entry.getCreationTime());
        assertEquals(startTime, entry.getLastModifiedTime());
        assertEquals(startTime, resource.getStores().getLastAccessStore().findLastAccessTime(entry));

        entries.stop();
    }

    /**
     * Separate test for something that is already compressed, to avoid
     * trying to compress again.
     */
    public void testLargerPrecompressed256k() throws Exception {
        _testLargerPrecompressed(256000);
    }

    public void testLargerPrecompressed2megs() throws Exception {
        _testLargerPrecompressed(2 * 1000 * 1000);
    }
        
    protected void _testLargerPrecompressed(final int origSize) throws Exception
    {
        final long startTime = 5000;
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(startTime);
        
        // Need to make big enough to use streaming, but should be compressible as well
        StoreResource<TestKey, StoredEntry<TestKey>> resource = createResource("largePrecomp"+origSize,
                timeMaster, true);
        final String BIG_STRING = biggerRandomData(origSize);
        final byte[] BIG_DATA_ORIG = BIG_STRING.getBytes("UTF-8");
        final byte[] BIG_DATA_PRECOMP = lzf(BIG_DATA_ORIG);
        
        // ok: assume empty Entity Store
        StorableStore entries = resource.getStores().getEntryStore();
        assertEquals(0, entries.getEntryCount());

        final TestKey INTERNAL_KEY1 = contentKey(CLIENT_ID, "data/bigPrecomp-1");

        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        assertEquals(404, response.getStatus());

        // then try adding said entry, without indicating compression
        FakeHttpRequest request = new FakeHttpRequest();
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(request, response,
                INTERNAL_KEY1, calcChecksum(BIG_DATA_PRECOMP), new ByteArrayInputStream(BIG_DATA_PRECOMP),
                null, null, null);

        // verify we got a file, and that its size is the same as what we passed
        assertSame(PutResponse.class, response.getEntity().getClass());
        PutResponse<?> presp = (PutResponse<?>) response.getEntity();
        if (response.getStatus() != 200) {
            fail("Expected 200 response, got "+response.getStatus()+"; message: "+presp.message);
        }

        assertFalse(presp.inlined);
        assertEquals(BIG_DATA_PRECOMP.length, presp.storageSize);
        assertEquals(-1, presp.size);

        // can we count on this getting updated? Seems to be, FWIW
        assertEquals(1, entries.getEntryCount());

        // Ok. Then, we should also be able to fetch it, right?
        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        assertEquals(200, response.getStatus());
        assertTrue(response.hasStreamingContent());
        // big enough, should be backed by a real file.
        assertTrue(response.hasFile());
        byte[] data = collectOutput(response);
        // regardless of compression (or lack thereof), should get same data back
        assertEquals(BIG_DATA_PRECOMP.length, data.length);
        Assert.assertArrayEquals(BIG_DATA_PRECOMP, data);

        StoredEntry<TestKey> entry = rawToEntry(entries.findEntry(StoreOperationSource.REQUEST,
                null, INTERNAL_KEY1.asStorableKey()));
        assertNotNull(entry);
        assertTrue(entry.hasExternalData());
        assertFalse(entry.hasInlineData());
        assertEquals(Compression.NONE, entry.getCompression());
        assertEquals(BIG_DATA_PRECOMP.length, entry.getStorageLength());
        assertEquals(-1, entry.getRaw().getOriginalLength());
        assertEquals(startTime, entry.getCreationTime());
        assertEquals(startTime, entry.getLastModifiedTime());
        assertEquals(startTime, resource.getStores().getLastAccessStore().findLastAccessTime(entry));

        entries.stop();
    }    
    /**
     * Test to verify that trying to send non-GZIP content, claiming to be GZIP,
     * fails.
     */
    public void testLargerGZIPEntryFailing() throws Exception
    {
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(1234L);
        
        // Need to make big enough to use streaming, but should be compressible as well
        int origSize = 256000;
        StoreResource<TestKey, StoredEntry<TestKey>> resource = createResource("largeGzipInvalid", timeMaster, true);
        final StorableStore entries = resource.getStores().getEntryStore();
        final String BIG_STRING = biggerSomewhatCompressibleData(origSize);
        final byte[] BIG_DATA_RAW = BIG_STRING.getBytes("UTF-8");

        final TestKey INTERNAL_KEY1 = contentKey(CLIENT_ID, "data/bigZ-Invalid-1");

        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        assertEquals(404, response.getStatus());

        // then try adding said entry, and letting server know it's gzip'd
        FakeHttpRequest request = new FakeHttpRequest()
            .addHeader("Content-Encoding", "gzip");
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(request, response,
                INTERNAL_KEY1, calcChecksum(BIG_DATA_RAW), new ByteArrayInputStream(BIG_DATA_RAW),
                null, null, null);

        // verify we get expected error
        assertSame(PutResponse.class, response.getEntity().getClass());
        PutResponse<?> presp = (PutResponse<?>) response.getEntity();
        if (response.getStatus() != 400) {
            fail("Expected 400 response, got "+response.getStatus()+"; message: "+presp.message);
        }
        verifyMessage("Bad compression", presp.message);

        entries.stop();
    }
}

