package com.fasterxml.clustermate.jaxrs.testutil;

import com.fasterxml.clustermate.service.lastaccess.LastAccessConverterBase;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.storemate.store.lastaccess.LastAccessUpdateMethod;

public class LastAccessConverterForTests
    extends LastAccessConverterBase<TestKey, StoredEntry<TestKey>>
{
    @Override
    public byte[] createLastAccessedKey(TestKey key, LastAccessUpdateMethod method)
    {
        FakeLastAccess acc = (FakeLastAccess) method;
        if (acc != null) {
            switch (acc) {
            case NONE:
                return null;
            case GROUPED:
                /* TestKey just has "customerId", no group; grouped by that id,
                 * all paths under single entry.
                 */
                int cid = key.getCustomerId().asInt();
                byte[] b = new byte[] { (byte) (cid>>24), (byte)(cid>>16), (byte)(cid>>8), (byte)cid };
                return b;
            case INDIVIDUAL: // whole key, including client id, group id length
                return key.asStorableKey().asBytes();
            }
        }
        return null;
    }
}
