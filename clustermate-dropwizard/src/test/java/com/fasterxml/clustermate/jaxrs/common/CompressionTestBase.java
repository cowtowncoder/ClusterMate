package com.fasterxml.clustermate.jaxrs.common;

import java.io.ByteArrayInputStream;

import org.junit.Assert;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.jaxrs.StoreResource;
import com.fasterxml.clustermate.jaxrs.testutil.*;
import com.fasterxml.clustermate.service.msg.PutResponse;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.StoreOperationSource;

/**
 * Test that verifies handling of pre-compressed content, and
 * content explicitly marked as "not to be compressed".
 */
public abstract class CompressionTestBase extends JaxrsStoreTestBase
{
    final static CustomerId CLIENT_ID = CustomerId.valueOf("COMP");

    @Override
    public void setUp() {
        initTestLogging();
    }

    protected abstract String testPrefix();

    public void testPreCompressedLZF() throws Exception
    {
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(1234L);

        StoreResource<TestKey, StoredEntry<TestKey>> resource = createResource(testPrefix(), timeMaster, true);

        // ensure it'll use gzip compression
        final String INPUT_STR = biggerCompressibleData(45000);
        final byte[] INPUT_STR_BYTES = INPUT_STR.getBytes("UTF-8");
        int origSize = INPUT_STR_BYTES.length;
        final byte[] INPUT_DATA_LZF = Compressors.lzfCompress(INPUT_STR_BYTES);
        
        // ok: assume empty Entity Store
        StorableStore entries = resource.getStores().getEntryStore();
        assertEquals(0, entryCount(entries));

        final TestKey INTERNAL_KEY1 = contentKey(CLIENT_ID,  "data/comp/1");

        // then try adding said entry
        FakeHttpRequest req = new FakeHttpRequest()
            .addHeader(ClusterMateConstants.HTTP_HEADER_COMPRESSION,
                Compression.LZF.asContentEncoding())
            .addHeader(ClusterMateConstants.CUSTOM_HTTP_HEADER_UNCOMPRESSED_LENGTH,
                String.valueOf(origSize));
        FakeHttpResponse response = new FakeHttpResponse();

        resource.getHandler().putEntry(req, response,
                INTERNAL_KEY1, calcChecksum(INPUT_DATA_LZF), new ByteArrayInputStream(INPUT_DATA_LZF),
                null, null, null);
        if (200 != response.getStatus()) {
//            String str = new String(response.getStreamingContentAsBytes(), "UTF-8");
            PutResponse<?> putResponse = response.getEntity();
            fail("Response ("+response.getStatus()+"), message: "+putResponse.message);
        }
        // verify we got a file...
        assertSame(PutResponse.class, response.getEntity().getClass());
        PutResponse<?> presp = (PutResponse<?>) response.getEntity();

        assertFalse(presp.inlined);

        // can we count on this getting updated? Seems to be, FWIW
        assertEquals(1, entryCount(entries));

        // first, verify store has it:
        StoredEntry<TestKey> entry = rawToEntry(entries.findEntry(StoreOperationSource.REQUEST,
                null, INTERNAL_KEY1.asStorableKey()));
        assertNotNull(entry);
        assertTrue(entry.hasExternalData());
        assertFalse(entry.hasInlineData());
        assertEquals(Compression.LZF, entry.getCompression());
        assertEquals(origSize, entry.getActualUncompressedLength());
        // compressed, should be smaller...
        assertTrue("Should be compressed; size="+entry.getActualUncompressedLength()+"; storage-size="+entry.getStorageLength(),
                entry.getActualUncompressedLength() > entry.getStorageLength());

        // Ok. Then, we should also be able to fetch it, right?
        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        assertEquals(200, response.getStatus());

        // big enough, should be backed by a real file...
        assertTrue(response.hasStreamingContent());
        assertTrue(response.hasFile());
        byte[] data = collectOutput(response);

        // and importantly, should also be auto-uncompressed as expected:
        assertEquals(origSize, data.length);
        Assert.assertArrayEquals(INPUT_STR_BYTES, data);
        
        entries.stop();
    }

    public void testExplicitDoNotCompress() throws Exception
    {
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(1234L);
        
        StoreResource<TestKey, StoredEntry<TestKey>> resource = createResource(testPrefix(), timeMaster, true);

        final String INPUT_STR = biggerCompressibleData(45000);
        final byte[] INPUT_STR_BYTES = INPUT_STR.getBytes("UTF-8");
        int origSize = INPUT_STR_BYTES.length;
        
        // ok: assume empty Entity Store
        StorableStore entries = resource.getStores().getEntryStore();
        assertEquals(0, entryCount(entries));

        final TestKey INTERNAL_KEY1 = contentKey(CLIENT_ID,  "data/comp-none/1");

        // then try adding said entry
        FakeHttpRequest req = new FakeHttpRequest()
            .addHeader(ClusterMateConstants.HTTP_HEADER_COMPRESSION,
                Compression.NONE.asContentEncoding());
        FakeHttpResponse response = new FakeHttpResponse();

        resource.getHandler().putEntry(req, response,
                INTERNAL_KEY1, calcChecksum(INPUT_STR_BYTES), new ByteArrayInputStream(INPUT_STR_BYTES),
                null, null, null);
        if (200 != response.getStatus()) {
            PutResponse<?> putResponse = response.getEntity();
            fail("Response ("+response.getStatus()+"), message: "+putResponse.message);
        }
        // verify we got a file...
        assertSame(PutResponse.class, response.getEntity().getClass());
        PutResponse<?> presp = (PutResponse<?>) response.getEntity();

        assertFalse(presp.inlined);

        // can we count on this getting updated? Seems to be, FWIW
        assertEquals(1, entryCount(entries));

        // first, verify store has it:
        StoredEntry<TestKey> entry = rawToEntry(entries.findEntry(StoreOperationSource.REQUEST,
                null, INTERNAL_KEY1.asStorableKey()));
        assertNotNull(entry);
        assertTrue(entry.hasExternalData());
        assertFalse(entry.hasInlineData());
        assertEquals(Compression.NONE, entry.getCompression());
        assertEquals(origSize, entry.getStorageLength());

        // Ok. Then, we should also be able to fetch it, right?
        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        assertEquals(200, response.getStatus());

        // big enough, should be backed by a real file...
        assertTrue(response.hasStreamingContent());
        assertTrue(response.hasFile());
        byte[] data = collectOutput(response);

        // and importantly, should also be auto-uncompressed as expected:
        assertEquals(origSize, data.length);
        Assert.assertArrayEquals(INPUT_STR_BYTES, data);
        
        entries.stop();
    }
}
