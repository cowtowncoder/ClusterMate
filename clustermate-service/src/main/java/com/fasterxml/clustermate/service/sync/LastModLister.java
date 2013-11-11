package com.fasterxml.clustermate.service.sync;

import java.util.ArrayList;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.api.KeyRange;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.backend.IterationAction;
import com.fasterxml.storemate.store.backend.StorableLastModIterationCallback;

class LastModLister<K extends EntryKey, E extends StoredEntry<K>>
    extends StorableLastModIterationCallback
{
    private final TimeMaster _timeMaster;
    private final StoredEntryConverter<K,E,?> _entryConverter;

    // // Limits
    
    private final KeyRange _inRange;

    private final EntryKeyConverter<K> _keyConverter;

    private final long _since, _upTo;

    private final long _processUntil;

    private final int _maxCount;
    
    // // Temporary values
    
    private K key = null;

    // // Result values
    
    private int _total = 0;

    private final ArrayList<E> _result;
    
    // last timestamp traversed that was in legal timestamp range
    private long _lastSeenValidTimestamp;

    // first timestamp out of valid range
    private long _nextTimestamp;

    // to ensure List advances timestamp:
    private boolean _timestampHasAdvanced = false;
    
    public LastModLister(TimeMaster timeMaster, StoredEntryConverter<K,E,?> entryConverter,
            KeyRange inRange, long since, long upTo, long processUntil, int maxCount,
            ArrayList<E> result)
    {
        _timeMaster = timeMaster;
        _entryConverter = entryConverter;
        _keyConverter = entryConverter.keyConverter();

        _inRange = inRange;
        _since = since;
        _upTo = upTo;
        _processUntil = processUntil;
        _maxCount = maxCount;

        _result = result;
    }
    
    /* We can do most efficient checks for timestamp range by
     * verifying timestamp first, right off the index we are
     * using...
     */
    @Override
    public IterationAction verifyTimestamp(long timestamp)
    {
        if (timestamp > _upTo) {
            /* 21-Sep-2012, tatu: Should we try to approximate latest
             *  possible "lastSeen" timestamp here? As long as we avoid
             *  in-flight-modifiable things, it would seem possible.
             *  However, let's play this safe for now.
             */
            _nextTimestamp = timestamp;
            return IterationAction.TERMINATE_ITERATION;
        }
        // First things first: we do want to know last seen entry that's "in range"
        _lastSeenValidTimestamp = timestamp;
        _timestampHasAdvanced |= (timestamp > _since);
        return IterationAction.PROCESS_ENTRY;
    }
    
    // Most of filtering can actually be done with just keys...
    @Override public IterationAction verifyKey(StorableKey rawKey)
    {
        // check time limits every 64 entries processed
        if ((++_total & 0x3F) == 0) {
            if (_timestampHasAdvanced &&
                    _timeMaster.realSystemTimeMillis() > _processUntil) {
                return IterationAction.TERMINATE_ITERATION;
            }
        }
        // and then verify that we are in range...
        key = _keyConverter.rawToEntryKey(rawKey);
        int hash = _keyConverter.routingHashFor(key);
        if (_inRange.contains(hash)) {
            return IterationAction.PROCESS_ENTRY;
        }
        return IterationAction.SKIP_ENTRY;
    }

    @Override
    public IterationAction processEntry(Storable storable)
    {
        E entry = _entryConverter.entryFromStorable(key, storable);
        _result.add(entry);
        /* One limitation, however; we MUST advance timer beyond initial
         * 'since' time. This may require including more than 'max' entries.
         */
        if (_timestampHasAdvanced && _result.size() >= _maxCount) {
            return IterationAction.TERMINATE_ITERATION;
        }
        return IterationAction.PROCESS_ENTRY;
    }

    public int getTotal() { return _total; }
    public long getLastSeenTimestamp() { return _lastSeenValidTimestamp; }
    public long getNextTimestamp() { return _nextTimestamp; }
}