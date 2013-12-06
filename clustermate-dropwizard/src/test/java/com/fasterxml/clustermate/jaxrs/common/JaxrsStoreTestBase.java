package com.fasterxml.clustermate.jaxrs.common;

import java.io.*;
import java.util.*;

import junit.framework.TestCase;

import org.skife.config.TimeSpan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.storemate.shared.IpAndPort;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.shared.compress.Compressors;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StorableStore;
import com.fasterxml.storemate.store.StoreException;
import com.fasterxml.storemate.store.backend.StoreBackend;
import com.fasterxml.storemate.store.file.DefaultFilenameConverter;
import com.fasterxml.storemate.store.file.FileManager;
import com.fasterxml.storemate.store.file.FileManagerConfig;
import com.fasterxml.storemate.store.impl.StorableStoreImpl;
import com.fasterxml.clustermate.api.KeySpace;
import com.fasterxml.clustermate.api.NodeDefinition;
import com.fasterxml.clustermate.jaxrs.testutil.*;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cfg.ClusterConfig;
import com.fasterxml.clustermate.service.cfg.NodeConfig;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServerImpl;
import com.fasterxml.clustermate.service.state.ActiveNodeState;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.std.ChecksumUtil;

/**
 * Shared base class for unit tests; contains shared utility methods.
 */
public abstract class JaxrsStoreTestBase extends TestCase
{
    /**
     * Let's use a "non-standard" port number for tests; could use ephemeral one,
     * but may be easier to troubleshoot if we use fixed one.
     * Lucky four sevens...
     */
    protected final static int TEST_PORT = 7777;
    
    // null -> require client id with key
    protected final TestKeyConverter _keyConverter = TestKeyConverter.defaultInstance(null);

    protected final StoredEntryConverterForTests _entryConverter = new StoredEntryConverterForTests(_keyConverter);

    protected final static ObjectMapper _mapper = new ObjectMapper();
    
    /*
    /**********************************************************************
    /* Configuration setting helpers
    /**********************************************************************
     */

    protected ServiceConfigForTests createSimpleTestConfig(String testSuffix, boolean cleanUp)
        throws IOException
    {
        // BDB and file store settings:
        File testRoot = getTestScratchDir(testSuffix, cleanUp);
        ServiceConfigForTests config = new ServiceConfigForTests();
        config.metadataDirectory = new File(testRoot, "bdb-cmtest");
        config.storeConfig.dataRootForFiles = new File(testRoot, "files");
        // shorten sync grace period to 5 seconds for tests:
        config.cfgSyncGracePeriod = new TimeSpan("5s");
        return config;
    }

    protected ServiceConfigForTests createSingleNodeConfig(String testSuffix,
            boolean cleanUp, int port)
        throws IOException
    {
        ServiceConfigForTests config = createSimpleTestConfig(testSuffix, cleanUp);

        ClusterConfig cluster = config.cluster;
        cluster.clusterKeyspaceSize = 360;
        ArrayList<NodeConfig> nodes = new ArrayList<NodeConfig>();
        nodes.add(new NodeConfig("localhost:"+TEST_PORT, 0, 360));
        // Cluster config? Set keyspace size, but nothing else yet
        cluster.clusterNodes = nodes;
        return config;
    }

    /*
    /**********************************************************************
    /* Store creation
    /**********************************************************************
     */

    protected abstract StoreBackend createBackend(ServiceConfig config, File fileDir);
    
    protected StoreResourceForTests<TestKey, StoredEntry<TestKey>>
    createResource(String testSuffix, TimeMaster timeMaster,
            boolean cleanUp)
        throws IOException
    {
        ServiceConfigForTests config = createSimpleTestConfig(testSuffix, cleanUp);
        File fileDir = config.storeConfig.dataRootForFiles;
        // can just use the default file manager impl...
        FileManager files = new FileManager(new FileManagerConfig(fileDir), timeMaster,
                new DefaultFilenameConverter());
        StoreBackend backend = createBackend(config, fileDir);
        // null -> use default throttler (simple)
        StorableStore store = new StorableStoreImpl(config.storeConfig,
                backend, timeMaster, files, null, null);
        SharedStuffForTests stuff = new SharedStuffForTests(config, timeMaster,
                _entryConverter, files);
        StoresForTests stores = new StoresForTests(config, timeMaster, stuff.jsonMapper(),
                _entryConverter, store, config.metadataDirectory);
        // important: configure to reduce log noise:
        stuff.markAsTest();
        stores.initAndOpen(false);
        return new StoreResourceForTests<TestKey, StoredEntry<TestKey>>(clusterViewForTesting(stuff, stores),
                        new StoreHandlerForTests(stuff, stores, null),
                        stuff);
    }

    /**
     * Bit unclean, but tests need to be able to create a light-weight instance
     * of cluster view.
     */
    protected ClusterViewByServerImpl<TestKey, StoredEntry<TestKey>> clusterViewForTesting(SharedServiceStuff stuff,
            StoresForTests stores)
    {
        KeySpace keyspace = new KeySpace(360);
        NodeDefinition localDef = new NodeDefinition(new IpAndPort("localhost:9999"), 1,
                keyspace.fullRange(), keyspace.fullRange());
        ActiveNodeState localState = new ActiveNodeState(localDef, 0L);
        return new ClusterViewByServerImpl<TestKey, StoredEntry<TestKey>>(stuff, stores, keyspace,
                localState,
                Collections.<IpAndPort,ActiveNodeState>emptyMap(),
                0L);
    }

    /*
    /**********************************************************************
    /* Other factory methods
    /**********************************************************************
     */
    
    protected TestKey contentKey(CustomerId clientId, String fullPath) {
        return _keyConverter.construct(clientId, fullPath);
    }

    protected TestKey contentKey(StorableKey raw) {
        return _keyConverter.rawToEntryKey(raw);
    }
    
    protected StoredEntry<TestKey> rawToEntry(Storable raw) {
        if (raw == null) {
    		    return null;
        }
        return _entryConverter.entryFromStorable(raw);
    }

    protected String toBase64(TestKey key) {
        return toBase64(key.asStorableKey());
    }
    
    protected String toBase64(StorableKey rawKey) {
        return _mapper.convertValue(rawKey.asBytes(), String.class);
    }

    /*
    /**********************************************************************
    /* Backend store access
    /**********************************************************************
     */

    protected long entryCount(StorableStore store) throws StoreException
    {
        if (store.getBackend().hasEfficientEntryCount()) {
            return store.getEntryCount();
        }
        return store.getBackend().countEntries();
    }

    protected long indexCount(StorableStore store) throws StoreException
    {
        if (store.getBackend().hasEfficientIndexCount()) {
            return store.getIndexedCount();
        }
        return store.getBackend().countIndexed();
    }
    
    /*
    /**********************************************************************
    /* Methods for file, directory handling
    /**********************************************************************
     */
	
    /**
     * Method for accessing "scratch" directory used for tests.
     * We'll try to create this directory under 
     * Assumption is that the current directory at this point
     * is project directory.
     */
    protected File getTestScratchDir(String testSuffix, boolean cleanUp) throws IOException
    {
        File f = new File(new File("test-data"), testSuffix).getCanonicalFile();
        if (!f.exists()) {
            if (!f.mkdirs()) {
                throw new IOException("Failed to create test directory '"+f.getAbsolutePath()+"'");
            }
        } else if (cleanUp) {
            for (File kid : f.listFiles()) {
                deleteFileOrDir(kid);
            }
        }
        return f;
    }

    protected void deleteFileOrDir(File fileOrDir) throws IOException
    {
        if (fileOrDir.isDirectory()) {
            for (File kid : fileOrDir.listFiles()) {
                deleteFileOrDir(kid);
            }
        }
        if (!fileOrDir.delete()) {
            throw new IOException("Failed to delete test file/directory '"+fileOrDir.getAbsolutePath()+"'");
        }
    }

    protected byte[] readAll(File f) throws IOException
    {
        FileInputStream in = new FileInputStream(f);
        byte[] data = readAll(in);
        in.close();
        return data;
    }

    protected byte[] readAll(InputStream in) throws IOException
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(4000);
        byte[] buf = new byte[4000];
        int count;
        while ((count = in.read(buf)) > 0) {
            bytes.write(buf, 0, count);
        }
        in.close();
        return bytes.toByteArray();
    }

    protected byte[] collectOutput(FakeHttpResponse response) throws IOException
    {
        assertTrue(response.hasStreamingContent());
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(4000);
        response.getStreamingContent().writeContent(bytes);
        return bytes.toByteArray();
    }
    
    /*
    /**********************************************************************
    /* Test methods: data generation
    /**********************************************************************
     */
	
    protected String biggerCompressibleData(int size)
    {
        StringBuilder sb = new StringBuilder(size + 100);
        final Random rnd = new Random(123);
        while (sb.length() < size) {
            sb.append("Some data: ")
            .append(sb.length())
            .append("/")
            .append(sb.length())
            .append(rnd.nextInt()).append("\n");
        }
        return sb.toString();
    }

    protected String biggerSomewhatCompressibleData(int size)
    {
        StringBuilder sb = new StringBuilder(size + 100);
        final Random rnd = new Random(123);
        while (sb.length() < size) {
            int val = rnd.nextInt();
            switch (val % 5) {
            case 0:
                sb.append('X');
                break;
            case 1:
                sb.append(": ").append(sb.length());
                break;
            case 2:
                sb.append('\n');
                break;
            case 3:
                sb.append((char) (33 + val & 0x3f));
                break;
            default:
                sb.append("/").append(Integer.toHexString(sb.length()));
                break;
            }
        }
        return sb.toString();
    }
    
    protected String biggerRandomData(int size)
    {
        StringBuilder sb = new StringBuilder(size + 100);
        Random rnd = new Random(size);
        for (int i = 0; i < size; ++i) {
            sb.append((byte) (32 + rnd.nextInt() % 95));
        }
        return sb.toString();
    }

    /*
    /**********************************************************************
    /* Test methods: message validation
    /**********************************************************************
     */

    protected void verifyException(Exception e, String expected)
    {
    	verifyMessage(expected, e.getMessage());
    }
    
    protected void verifyMessage(String expectedPiece, String actual)
    {
        if (actual == null || actual.toLowerCase().indexOf(expectedPiece.toLowerCase()) < 0) {
            fail("Expected message that contains phrase '"+expectedPiece+"'; instead got: '"
                    +actual+"'");
        }
    }
	
    /*
    /**********************************************************************
    /* Log setup
    /**********************************************************************
     */

    /**
     * Method to be called before tests, to ensure log4j does not whine.
     */
    protected void initTestLogging()
    {
//        Log.named(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(Level.WARN);
    }

    /*
    /**********************************************************************
    /* Checksum calculation
    /**********************************************************************
     */

    protected int calcChecksum(byte[] data) {
        return ChecksumUtil.calcChecksum(data);
    }

    protected byte[] lzf(byte[] input) throws IOException
    {
        return Compressors.lzfCompress(input);
    }
    
    protected byte[] gzip(byte[] input) throws IOException
    {
        return Compressors.gzipCompress(input);
    }

    protected byte[] gunzip(byte[] comp) throws IOException
    {
        return Compressors.gzipUncompress(comp);
    }
}
