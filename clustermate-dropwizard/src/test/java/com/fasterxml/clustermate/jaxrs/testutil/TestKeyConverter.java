package com.fasterxml.clustermate.jaxrs.testutil;

import com.fasterxml.clustermate.api.DecodableRequestPath;
import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.api.RequestPathBuilder;
import com.fasterxml.storemate.shared.*;
import com.fasterxml.storemate.shared.hash.BlockHasher32;
import com.fasterxml.storemate.shared.hash.BlockMurmur3Hasher;
import com.fasterxml.storemate.shared.hash.IncrementalHasher32;
import com.fasterxml.storemate.shared.hash.IncrementalMurmur3Hasher;
import com.fasterxml.storemate.shared.util.UTF8Encoder;
import com.fasterxml.storemate.shared.util.WithBytesCallback;

/**
 * Test implementation of {@link EntryKeyConverter}.
 *<p>
 * In addition class implements encoding and decoding of the entry keys
 * (of type {@link TestKey}): uses {@link StoreTestConstants#V_QUERY_PARAM_PARTITION_ID}
 * and {@link StoreTestConstants#V_QUERY_PARAM_NAMESPACE_ID} in addition to path
 * (which is regular filename).
 */
public class TestKeyConverter
    extends EntryKeyConverter<TestKey>
{
    /**
     * Keys have 4-byte fixed prefix, to include customer id.
     */
    public final static int DEFAULT_KEY_HEADER_LENGTH = 4;

    /**
     * Client id default value used if no value found from request path;
     * if null, client id is not optional.
     */
    protected final CustomerId _defaultCustomer;

    /**
     * Object we use for calculating high-quality hash codes, both for routing (for keys)
     * and for content.
     */
    protected final BlockHasher32 _hasher;
    
    /*
    /**********************************************************************
    /* Life-cycle of converter
    /**********************************************************************
     */

    protected TestKeyConverter(CustomerId defaultClientId) {
        this(defaultClientId, new BlockMurmur3Hasher());
    }
    
    protected TestKeyConverter(CustomerId defaultCustomer, BlockHasher32 blockHasher)
    {
        _defaultCustomer = defaultCustomer;
        _hasher = blockHasher;
    }
    
    /**
     * Accessor for getting default converter instance.
     */
    public static TestKeyConverter defaultInstance(CustomerId defaultClientId) {
        return new TestKeyConverter(defaultClientId);
    }

    /*
    /**********************************************************************
    /* Basic EntryKeyConverter impl
    /**********************************************************************
     */

    @Override
    public TestKey construct(byte[] rawKey) {
        return rawToEntryKey(new StorableKey(rawKey));
    }

    @Override
    public TestKey construct(byte[] rawKey, int offset, int length) {
        return rawToEntryKey(new StorableKey(rawKey, offset, length));
    }
    
    @Override
    public TestKey rawToEntryKey(final StorableKey rawKey) {
        return rawKey.with(new WithBytesCallback<TestKey>() {
            @Override
            public TestKey withBytes(byte[] buffer, int offset, int length) {
                int cid = (buffer[offset] << 24)
                        | ((buffer[++offset] & 0xFF) << 16)
                        | ((buffer[++offset] & 0xFF) << 8)
                        | (buffer[++offset] & 0xFF)
                        ;
                return new TestKey(rawKey, CustomerId.valueOf(cid));
            }
        });
    }

    @Override
    public TestKey stringToKey(String external) {
        if (!external.startsWith(TestKey.TEST_KEY_PREFIX)) {
            throw new IllegalArgumentException("Keys must start with prefix, got: "+external);
        }
        external = external.substring(TestKey.TEST_KEY_PREFIX.length());
        int ix = external.indexOf(TestKey.TEST_KEY_SEPARATOR);
        if (ix < 0) {
            throw new IllegalArgumentException("Key missing separator: "+external);
        }
        return construct(CustomerId.valueOf(external.substring(0, ix)), external.substring(ix+1));
    }

    @Override
    public String keyToString(TestKey key) {
        // works here, unlike with many real key types
        return key.toString();
    }

    @Override
    public String rawToString(StorableKey key) {
        // could be optimized, but no point for tests
        return keyToString(rawToEntryKey(key));
    }
    
    /**
     * Method called to figure out raw hash code to use for routing request
     * regarding given content key.
     */
    @Override
    public int routingHashFor(TestKey key) {
        return _truncateHash(rawHashForRouting(key, _hasher));
    }

    /*
    /**********************************************************************
    /* Key construction, conversions
    /**********************************************************************
     */

    public TestKey construct(CustomerId customerId, String fullPath)
    {
        byte[] b = UTF8Encoder.encodeAsUTF8(fullPath, DEFAULT_KEY_HEADER_LENGTH, 0, false);
        customerId.append(b, 0);
        return new TestKey(new StorableKey(b), customerId);
    }

    public StorableKey storableKey(CustomerId clientId, String fullPath)
    {
        byte[] b = UTF8Encoder.encodeAsUTF8(fullPath, DEFAULT_KEY_HEADER_LENGTH, 0, false);
        clientId.append(b, 0);
        return new StorableKey(b);
    }

    /*
    /**********************************************************************
    /* Path handling
    /**********************************************************************
     */
    
    @SuppressWarnings("unchecked")
    @Override
    public <B extends RequestPathBuilder> B appendToPath(B b, TestKey key)
    {
        /* ClientId: could either add as a segment, or query param.
         * For now, do latter.
         */
        b = (B) b.addParameter(StoreTestConstants.V_QUERY_PARAM_CUSTOMER_ID, key.getCustomerId().toString());
        String path = key.getExternalPath();
        // also: while not harmful, let's avoid escaping embedded slashes (slightly more compact)
        b = (B) b.addPathSegmentsRaw(path);
        return b;
    }

    @Override
    public <P extends DecodableRequestPath> TestKey extractFromPath(P path)
    {
        String customerStr = path.getQueryParameter(StoreTestConstants.V_QUERY_PARAM_CUSTOMER_ID);
        CustomerId customerId;
        if (customerStr == null || customerStr.length() == 0) {
            if (_defaultCustomer == null) {
                throw new IllegalArgumentException("No ClientId found from path: "+path);
            }
            customerId = _defaultCustomer;
        } else {
            customerId = CustomerId.valueOf(customerStr);
        }
        final String filename = path.getDecodedPath();
//System.err.println("Non-group key: path ("+filename.length()+"): '"+filename+"'");
        return construct(customerId, filename);
    }

    /*
    /**********************************************************************
    /* Hash code calculation
    /**********************************************************************
     */

    protected int rawHashForRouting(TestKey key, BlockHasher32 hasher) {
        // simple for now; only use customerId for routing
        return key.getCustomerId().hashCode();
    }

    @Override
    public int contentHashFor(ByteContainer bytes)
    {
        return bytes.hash(_hasher, BlockHasher32.DEFAULT_SEED);
    }

    /**
     * Method that will create a <b>new</b> hasher instance for calculating
     * hash values for content that can not be handled as a single block.
     */
    @Override
    public IncrementalHasher32 createStreamingContentHasher() {
        return new IncrementalMurmur3Hasher();
    }
}
