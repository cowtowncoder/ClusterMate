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
     * Keys have 6-byte fixed prefix, to include clientId and information
     * on group id length.
     */
    public final static int DEFAULT_KEY_HEADER_LENGTH = 6;

    public final static int MAX_NAMESPACE_BYTE_LENGTH = 0x7FFF;

    /**
     * Client id default value used if no value found from request path;
     * if null, client id is not optional.
     */
    protected final PartitionId _defaultPartition;

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

    protected TestKeyConverter(PartitionId defaultClientId) {
        this(defaultClientId, new BlockMurmur3Hasher());
    }
    
    protected TestKeyConverter(PartitionId defaultPartition, BlockHasher32 blockHasher)
    {
        _defaultPartition = defaultPartition;
        _hasher = blockHasher;
    }
    
    /**
     * Accessor for getting default converter instance.
     */
    public static TestKeyConverter defaultInstance(PartitionId defaultClientId) {
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
                int groupIdLength = ((buffer[++offset] & 0xFF) << 8)
                        | (buffer[++offset] & 0xFF);
                return new TestKey(rawKey, PartitionId.valueOf(cid), groupIdLength);
            }
        });
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

    /**
     * Method called to construct a {@link TestKey}
     * that does not have group id.
     */
    public TestKey construct(PartitionId clientId, String fullPath) {
        return construct(clientId, fullPath, 0);
    }
    
    public TestKey construct(PartitionId clientId, String fullPath,
            int groupIdLengthInBytes)
    {
        if (groupIdLengthInBytes > MAX_NAMESPACE_BYTE_LENGTH) {
            throw new IllegalArgumentException("Group id byte length too long ("+groupIdLengthInBytes
                    +"), can not exceed "+MAX_NAMESPACE_BYTE_LENGTH);
        }
        byte[] b = UTF8Encoder.encodeAsUTF8(fullPath, DEFAULT_KEY_HEADER_LENGTH, 0, false);
        clientId.append(b, 0);
        // group id length is a positive 16-bit short, MSB:
        b[4] = (byte) (groupIdLengthInBytes >> 8);
        b[5] = (byte) groupIdLengthInBytes;

        return new TestKey(new StorableKey(b), clientId, groupIdLengthInBytes);
    }

    /**
     * Method called to construct a {@link TestKey} given a two-part
     * path; group id as prefix, and additional path scoped by group id.
     */
    public TestKey construct(PartitionId clientId, String groupId,
            String path)
    {
        // sanity check for "no group id" case
        if (groupId == null || groupId.length() == 0) {
            return construct(clientId, path);
        }
        
        byte[] prefixPart = UTF8Encoder.encodeAsUTF8(groupId, DEFAULT_KEY_HEADER_LENGTH, 0, false);
        final int groupIdLengthInBytes = prefixPart.length - DEFAULT_KEY_HEADER_LENGTH;
        if (groupIdLengthInBytes > MAX_NAMESPACE_BYTE_LENGTH) {
            throw new IllegalArgumentException("Group id byte length too long ("+groupIdLengthInBytes
                    +"), can not exceed "+MAX_NAMESPACE_BYTE_LENGTH);
        }
        clientId.append(prefixPart, 0);
        prefixPart[4] = (byte) (groupIdLengthInBytes >> 8);
        prefixPart[5] = (byte) groupIdLengthInBytes;
        
        // so far so good: and now append actual path
        byte[] fullKey = UTF8Encoder.encodeAsUTF8(prefixPart, path);
        return new TestKey(new StorableKey(fullKey), clientId, groupIdLengthInBytes);
    }

    public StorableKey storableKey(PartitionId clientId, String fullPath, int groupIdLengthInBytes)
    {
        if (groupIdLengthInBytes > MAX_NAMESPACE_BYTE_LENGTH) {
            throw new IllegalArgumentException("Group id byte length too long ("+groupIdLengthInBytes
                    +"), can not exceed "+MAX_NAMESPACE_BYTE_LENGTH);
        }
        byte[] b = UTF8Encoder.encodeAsUTF8(fullPath, DEFAULT_KEY_HEADER_LENGTH, 0, false);
        clientId.append(b, 0);
        // group id length is a positive 16-bit short, MSB:
        b[4] = (byte) (groupIdLengthInBytes >> 8);
        b[5] = (byte) groupIdLengthInBytes;

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
        b = (B) b.addParameter(StoreTestConstants.V_QUERY_PARAM_PARTITION_ID, key.getPartitionId().toString());
        String path = key.getExternalPath();
        int groupLen = key.getGroupIdLength();
        if (groupLen > 0) {
            b = (B) b.addParameter(StoreTestConstants.V_QUERY_PARAM_NAMESPACE_ID, key.getGroupId());
            // and then remove group-id part
            path = path.substring(groupLen);
        }
        // also: while not harmful, let's avoid escaping embedded slashes (slightly more compact)
        b = (B) b.addPathSegmentsRaw(path);
        return b;
    }

    @Override
    public <P extends DecodableRequestPath> TestKey extractFromPath(P path)
    {
        String clientIdStr = path.getQueryParameter(StoreTestConstants.V_QUERY_PARAM_PARTITION_ID);
        PartitionId clientId;
        if (clientIdStr == null || clientIdStr.length() == 0) {
            if (_defaultPartition == null) {
                throw new IllegalArgumentException("No ClientId found from path: "+path);
            }
            clientId = _defaultPartition;
        } else {
            clientId = PartitionId.valueOf(clientIdStr);
        }
        final String groupId = path.getQueryParameter(StoreTestConstants.V_QUERY_PARAM_NAMESPACE_ID);
        // but ignore empty one
        final String filename = path.getDecodedPath();
        if (groupId != null) {
            if (groupId.length() > 0) {
//System.err.println("Group key: path ("+filename.length()+"): '"+filename+"'; group '"+groupId+"'");
                return construct(clientId, groupId, filename);
            }
        }
//System.err.println("Non-group key: path ("+filename.length()+"): '"+filename+"'");
        return construct(clientId, filename);
    }

    /*
    /**********************************************************************
    /* Hash code calculation
    /**********************************************************************
     */

    protected int rawHashForRouting(TestKey key, BlockHasher32 hasher)
    {
        // first: skip metadata (clientId, group id length indicator)
        int offset = TestKeyConverter.DEFAULT_KEY_HEADER_LENGTH;
        StorableKey rawKey = key.asStorableKey();
        int length = rawKey.length() - offset;
        // second include only group id (if got one), or full thing
        if (key.hasGroupId()) {
            length = key.getGroupIdLength();
        }
        return rawKey.hashCode(hasher, offset, length);
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
