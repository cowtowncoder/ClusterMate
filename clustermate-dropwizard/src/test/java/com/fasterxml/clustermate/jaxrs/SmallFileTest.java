package com.fasterxml.clustermate.jaxrs;

import java.io.*;

import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;
import com.fasterxml.storemate.shared.hash.HashConstants;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.StoreOperationSource;
import com.fasterxml.storemate.store.util.OperationDiagnostics;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.jaxrs.StoreResource;
import com.fasterxml.clustermate.jaxrs.testutil.*;
import com.fasterxml.clustermate.service.msg.PutResponse;
import com.fasterxml.clustermate.service.store.StoredEntry;

/**
 * Basic testing of creating some data from scratch, accessing it.
 */
public class SmallFileTest extends JaxrsStoreTestBase
{
    final static CustomerId CLIENT_ID = CustomerId.valueOf(1234);

    final static String TEST_PREFIX = "small";

    @Override
    public void setUp() {
        initTestLogging();
    }
	
    public void testSmallFile() throws Exception
    {
        final long creationTime = 1234L;
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(creationTime);

        StoreResource<TestKey, StoredEntry<TestKey>> resource = createResource(TEST_PREFIX, timeMaster, true);

        // ok: assume empty Entity Store
        StorableStore entries = resource.getStores().getEntryStore();
        assertEquals(0, entries.getEntryCount());

        // then try to find bogus entry; make sure to use a slash...
        final TestKey INTERNAL_KEY1 = contentKey(CLIENT_ID, "data/entry/1");
        final String SMALL_STRING = "Some data that we want to store -- small, gets inlined...";
        final byte[] SMALL_DATA = SMALL_STRING.getBytes("UTF-8");

        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        assertEquals(404, response.getStatus());

        // and more fundamentally, verify store had it:
        // then try adding said entry
        response = new FakeHttpResponse();
        final int inputHash = calcChecksum(SMALL_DATA);
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                INTERNAL_KEY1, inputHash, new ByteArrayInputStream(SMALL_DATA),
                null, null, null);
        assertEquals(200, response.getStatus());
        // can we count on this getting updated? Seems to be, FWIW
        assertEquals(1, entries.getEntryCount());
        // not accessed yet:
        assertEquals(0L, resource.getStores().getLastAccessStore().findLastAccessTime(INTERNAL_KEY1,
                FakeLastAccess.INDIVIDUAL));

        // Ok. Then, we should also be able to fetch it, right?
        response = new FakeHttpResponse();
        // and ensure we get diagnostics too
        OperationDiagnostics stats = new OperationDiagnostics(0L);
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1, stats);
        assertNotNull(stats.getEntry());
        assertEquals(200, response.getStatus());

        // As per Issue #6, we should get Etag back:
        String etag = response.getHeader(ClusterMateConstants.HTTP_HEADER_ETAG);
        String expEtag = "\"" + inputHash + "\"";
        assertEquals(expEtag, etag);
        
        /*
        StatsCollectingOutputStream statsOut = new StatsCollectingOutputStream(new ByteArrayOutputStream());
        response.getStreamingContent().writeContent(statsOut);
        assertEquals(SMALL_DATA.length, statsOut.getBytesWritten());
        */

        // and now last-accessed should be set
        assertEquals(1234L, resource.getStores().getLastAccessStore().findLastAccessTime(INTERNAL_KEY1,
                FakeLastAccess.INDIVIDUAL));

        // let's verify it then; small request...
        assertTrue(response.hasStreamingContent());
        assertTrue(response.hasInlinedData());
        byte[] data = collectOutput(response);
        assertEquals(SMALL_STRING, new String(data, "UTF-8"));
        
        Storable raw = entries.findEntry(StoreOperationSource.REQUEST, null, INTERNAL_KEY1.asStorableKey());
        assertNotNull(raw);
        // too small to be compressed, so:
        assertEquals(Compression.NONE, raw.getCompression());
        assertFalse(raw.hasExternalData());
        assertTrue(raw.hasInlineData());
        assertEquals(SMALL_DATA.length, raw.getInlineDataLength());

        StoredEntry<TestKey> entry = rawToEntry(raw);

        assertEquals(creationTime, entry.getCreationTime());
        assertEquals(creationTime, entry.getLastModifiedTime());

        // one more access; this time to modify last accessed
        long accessTime = timeMaster.advanceCurrentTimeMillis(999L).currentTimeMillis();
        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        assertEquals(200, response.getStatus());

        // true->should update last-accessed timestamp
        raw = entries.findEntry(StoreOperationSource.REQUEST,
                null, INTERNAL_KEY1.asStorableKey());
        assertNotNull(entry);
        assertEquals(accessTime, resource.getStores().getLastAccessStore().findLastAccessTime(entry));

        // and as per Issue #7: should be able to use Conditional GET too:
        response = new FakeHttpResponse();
        stats = new OperationDiagnostics(0L);
        FakeHttpRequest req = new FakeHttpRequest();
        req.addHeader(ClusterMateConstants.HTTP_HEADER_ETAG_NO_MATCH,  expEtag);
        resource.getHandler().getEntry(req, response, INTERNAL_KEY1, stats);
        assertNotNull(stats.getEntry());
        assertEquals(304, response.getStatus());

        // but Etag must match so
        response = new FakeHttpResponse();
        stats = new OperationDiagnostics(0L);
        req = new FakeHttpRequest();
        req.addHeader(ClusterMateConstants.HTTP_HEADER_ETAG_NO_MATCH,  "\"1234\"");
        resource.getHandler().getEntry(req, response, INTERNAL_KEY1, stats);
        assertNotNull(stats.getEntry());
        assertEquals(200, response.getStatus());
        
        // need to also close things after done, to exit test
        entries.stop();
    }

    // Test to verify storage of 0-length entry
    public void testEmptyFile() throws Exception
    {
        final long creationTime = 456L;
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(creationTime);

        StoreResource<TestKey, StoredEntry<TestKey>> resource = createResource(TEST_PREFIX, timeMaster, true);

        StorableStore entries = resource.getStores().getEntryStore();
        assertEquals(0, entries.getEntryCount());

        final TestKey INTERNAL_KEY1 = contentKey(CLIENT_ID, "data/entry/0");
        final byte[] SMALL_DATA = new byte[0];

        FakeHttpResponse response = new FakeHttpResponse();
        final int inputHash = calcChecksum(SMALL_DATA);
        
        // and just to make sure... we must NOT be using "no checksum" here
        if (inputHash == HashConstants.NO_CHECKSUM) {
            fail("Checksum must not be "+HashConstants.NO_CHECKSUM);
        }
        
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                INTERNAL_KEY1, inputHash, new ByteArrayInputStream(SMALL_DATA),
                null, null, null);
        if (response.getStatus() != 200) {
            PutResponse<?> presp = (PutResponse<?>) response.getEntity();
            fail("Failed with response code of "+response.getStatus()+"; fail="+presp.message);
        }
        assertEquals(1, entries.getEntryCount());
        assertEquals(0L, resource.getStores().getLastAccessStore().findLastAccessTime(INTERNAL_KEY1,
                FakeLastAccess.INDIVIDUAL));

        // Ok. Then, we should also be able to fetch it, right?
        response = new FakeHttpResponse();
        // and ensure we get diagnostics too
        OperationDiagnostics stats = new OperationDiagnostics(0L);
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1, stats);
        assertNotNull(stats.getEntry());
        assertEquals(200, response.getStatus());

        // As per Issue #6, we should get Etag back:
        String etag = response.getHeader(ClusterMateConstants.HTTP_HEADER_ETAG);
        String expEtag = "\"" + inputHash + "\"";
        assertEquals(expEtag, etag);

        // and now last-accessed should be set
        assertEquals(creationTime, resource.getStores().getLastAccessStore().findLastAccessTime(INTERNAL_KEY1,
                FakeLastAccess.INDIVIDUAL));

        // let's verify it then; small request...
        assertTrue(response.hasStreamingContent());
        assertTrue(response.hasInlinedData());
        byte[] data = collectOutput(response);
        assertEquals(0, data.length);
        
        Storable raw = entries.findEntry(StoreOperationSource.REQUEST, null, INTERNAL_KEY1.asStorableKey());
        assertNotNull(raw);
        // too small to be compressed, so:
        assertEquals(Compression.NONE, raw.getCompression());
        assertFalse(raw.hasExternalData());
        assertFalse(raw.hasInlineData()); // considered "no inline data"
        assertEquals(SMALL_DATA.length, raw.getInlineDataLength());

        StoredEntry<TestKey> entry = rawToEntry(raw);

        assertEquals(creationTime, entry.getCreationTime());
        assertEquals(creationTime, entry.getLastModifiedTime());

        entries.stop();
    }
    
    /**
     * Slight variation of basic test: let's accept compressed
     * contents; verify that we are served compressed thing
     */
    public void testSmallFileAcceptGzip() throws Exception
    {
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(1234L);

        StoreResource<TestKey, StoredEntry<TestKey>> resource = createResource(TEST_PREFIX+"gzip", timeMaster, true);

        // ok: assume empty Entity Store
        StorableStore entries = resource.getStores().getEntryStore();
        assertEquals(0, entries.getEntryCount());

        final String KEY1 = "data/entry/2";
        final String SMALL_STRING = this.biggerCompressibleData(400);
        final byte[] SMALL_DATA = SMALL_STRING.getBytes("UTF-8");
        final String ACCEPTED_ENCODING = "lzf,gzip";
        TestKey INTERNAL_KEY1 = contentKey(CLIENT_ID, KEY1);

        final long creationTime = timeMaster.advanceCurrentTimeMillis(1000L).currentTimeMillis();
        
        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest().setAcceptedCompression(ACCEPTED_ENCODING),
                response, INTERNAL_KEY1);
        assertEquals(404, response.getStatus());

        // then try adding said entry
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                INTERNAL_KEY1, calcChecksum(SMALL_DATA), new ByteArrayInputStream(SMALL_DATA),
                null, null, null);
        assertEquals(200, response.getStatus());
        // can we count on this getting updated? Seems to be, FWIW
        assertEquals(1, entries.getEntryCount());

        // Ok. Then, we should also be able to fetch it, right?
        response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest().setAcceptedCompression(ACCEPTED_ENCODING),
                response, INTERNAL_KEY1);
        assertEquals(200, response.getStatus());

        // let's verify it then; small request...
        assertTrue(response.hasStreamingContent());

        // check headers first: most importantly, must declare that we are served gzipped stuff:
        assertEquals("gzip", response.getHeader("Content-Encoding"));

        // and should also get something for 'last cluster update', although no real state:
        /* 11-Jul-2012, tatu: Actually this only gets added if we go through actual
         *   resource, and not store (store has no concept of cluster, or access).
         *   Need to figure out better place to do this.
         */
        /*
        Object value = response.getHeader(Constants.HTTP_HEADER_LAST_CLUSTER_UPDATE);
        assertNotNull(value);
        */
        assertTrue(response.hasInlinedData());
        byte[] compData = collectOutput(response);
        final int COMP_LEN = compData.length;
        assertTrue(COMP_LEN < SMALL_DATA.length);
        byte[] data = Compressors.gzipUncompress(compData);

        assertEquals(SMALL_DATA.length, data.length);
        assertEquals(SMALL_STRING, new String(data, "UTF-8"));

        // and more fundamentally, verify store had it:
        Storable raw = entries.findEntry(StoreOperationSource.REQUEST,
                null, INTERNAL_KEY1.asStorableKey());
        StoredEntry<TestKey> entry = rawToEntry(raw);
        assertNotNull(entry);
        // too small to be compressed, so:
        assertEquals(Compression.GZIP, entry.getCompression());
        assertFalse(entry.hasExternalData());
        assertTrue(entry.hasInlineData());
        assertEquals(COMP_LEN, raw.getInlineDataLength());
        assertEquals(COMP_LEN, entry.getStorageLength());
        assertEquals(SMALL_DATA.length, raw.getOriginalLength());
        assertEquals(creationTime, entry.getCreationTime());
        assertEquals(creationTime, entry.getLastModifiedTime());
        assertEquals(creationTime, resource.getStores().getLastAccessStore().findLastAccessTime(entry));
        
        // need to also close things after done, to exit test
        resource.getStores().stop();
    }
    
    /**
     * Test to verify handling of duplicate PUTs
     */
    public void testDuplicates() throws Exception
    {
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(1234L);
		
        StoreResource<TestKey, StoredEntry<TestKey>> resource = createResource(TEST_PREFIX+"dup", timeMaster, true);
        StorableStore entries = resource.getStores().getEntryStore();
        assertEquals(0, entries.getEntryCount());

        final String KEY1 = "data";
        final String SMALL_STRING = "Some smallish data...";
        final byte[] SMALL_DATA = SMALL_STRING.getBytes("UTF-8");
        TestKey INTERNAL_KEY1 = contentKey(CLIENT_ID, KEY1);

        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                INTERNAL_KEY1, calcChecksum(SMALL_DATA),
                new ByteArrayInputStream(SMALL_DATA),
                null, null, null);
        assertEquals(200, response.getStatus());
        // can we count on this getting updated? Seems to be, FWIW
        assertEquals(1, entries.getEntryCount());

        // Ok: first, is ok to try to PUT again
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                INTERNAL_KEY1, calcChecksum(SMALL_DATA), new ByteArrayInputStream(SMALL_DATA),
                null, null, null);
        if (response.getStatus() != 200) {
            PutResponse<?> presp = (PutResponse<?>) response.getEntity();
            fail("Failed with response code of "+response.getStatus()+"; fail="+presp.message);
        }
        // but no more entries are created
        assertEquals(1, entries.getEntryCount());

        // But not OK if checksum is different
        final String SMALL_STRING2 = "Other data";
        final byte[] SMALL_DATA2 = SMALL_STRING2.getBytes("UTF-8");
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                INTERNAL_KEY1, calcChecksum(SMALL_DATA2), new ByteArrayInputStream(SMALL_DATA2),
                null, null, null);
        // 409 == CONFLICT, due to mismatch of content checksums
        assertEquals(409, response.getStatus());
        PutResponse<?> pr = (PutResponse<?>) response.getEntity();
        verifyMessage("Failed PUT: trying to overwrite entry", pr.message);
        assertEquals(1, entries.getEntryCount());

        entries.stop();
    }

    /**
     * Test to check that if we already have LZF, we don't even try to re-compress it
     */
    public void testSmallTextAlreadyLZF() throws Exception
    {
        final long startTime = 1234L;
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(startTime);

        StoreResource<TestKey, StoredEntry<TestKey>> resource = createResource(TEST_PREFIX+"lzf", timeMaster, true);
        final StorableStore entries = resource.getStores().getEntryStore();
        final String SMALL_STRING = this.biggerCompressibleData(400);
        final byte[] SMALL_DATA_ORIG = SMALL_STRING.getBytes("UTF-8");
        final byte[] SMALL_DATA_LZF = Compressors.lzfCompress(SMALL_DATA_ORIG, 0, SMALL_DATA_ORIG.length);
        final TestKey INTERNAL_KEY1 = contentKey(CLIENT_ID, "data/small-LZF-1");

        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        assertEquals(404, response.getStatus());

        // Let's NOT indicate as compressed, should figure it out
        // and then verify that it is stored as if not compressed
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(new FakeHttpRequest(), response,
                INTERNAL_KEY1, calcChecksum(SMALL_DATA_LZF), new ByteArrayInputStream(SMALL_DATA_LZF),
                null, null, null);

        // verify we get expected error
        assertSame(PutResponse.class, response.getEntity().getClass());
        assertEquals(200, response.getStatus());

        Storable entry = entries.findEntry(StoreOperationSource.REQUEST,
                null, INTERNAL_KEY1.asStorableKey());
        assertNotNull(entry);
        // too small to be compressed, so:
        assertEquals(Compression.NONE, entry.getCompression());
        assertFalse(entry.hasExternalData());
        assertTrue(entry.hasInlineData());
        assertEquals(SMALL_DATA_LZF.length, entry.getInlineDataLength());
        assertEquals(SMALL_DATA_LZF.length, entry.getStorageLength());
        // not compressed, so we get a marker (-1)
        assertEquals(-1L, entry.getOriginalLength());
        
        entries.stop();
    }

    
    /**
     * Test to verify that trying to send non-LZF content, claiming to be LZF,
     * fails.
     */
    public void testSmallTextAllegedLZF() throws Exception
    {
        final TimeMasterForSimpleTesting timeMaster = new TimeMasterForSimpleTesting(1234L);

        StoreResource<TestKey, StoredEntry<TestKey>> resource = createResource(TEST_PREFIX+"lzfFail", timeMaster, true);
        final StorableStore entries = resource.getStores().getEntryStore();
        final String STRING = "ZV but not really LZF";
        final byte[] STRING_BYTES = STRING.getBytes("UTF-8");
        TestKey INTERNAL_KEY1 = contentKey(CLIENT_ID, "data/smallLZF-invalid-1");

        FakeHttpResponse response = new FakeHttpResponse();
        resource.getHandler().getEntry(new FakeHttpRequest(), response, INTERNAL_KEY1);
        assertEquals(404, response.getStatus());

        // then try adding said entry, and claiming it's LZF encoded
        FakeHttpRequest request = new FakeHttpRequest()
            .addHeader("Content-Encoding", "lzf");
        response = new FakeHttpResponse();
        resource.getHandler().putEntry(request, response,
                INTERNAL_KEY1, calcChecksum(STRING_BYTES), new ByteArrayInputStream(STRING_BYTES),
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
