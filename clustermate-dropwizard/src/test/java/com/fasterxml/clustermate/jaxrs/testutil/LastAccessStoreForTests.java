package com.fasterxml.clustermate.jaxrs.testutil;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;

import com.fasterxml.clustermate.service.LastAccessUpdateMethod;
import com.fasterxml.clustermate.service.bdb.BDBConverters;
import com.fasterxml.clustermate.service.bdb.LastAccessStore;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;

public class LastAccessStoreForTests
    extends LastAccessStore<TestKey, StoredEntry<TestKey>>
{
    public LastAccessStoreForTests(Environment env, StoredEntryConverter<TestKey,StoredEntry<TestKey>> conv) {
        super(env, conv);
    }

    @Override
    protected DatabaseEntry lastAccessKey(TestKey key, LastAccessUpdateMethod acc)
    {
        if (acc != null) {
            switch (acc) {
            case NONE:
                return null;
            case GROUPED: // important: not just group id, but also client id
                return key.withGroupPrefix(BDBConverters.simpleConverter);
            case INDIVIDUAL: // whole key, including client id, group id length
                return key.asStorableKey().with(BDBConverters.simpleConverter);
            }
        }
        return null;
    }
}
