package com.fasterxml.clustermate.api.msg;

import java.util.*;

import com.fasterxml.jackson.annotation.JsonAnyGetter;

/**
 * Base class used by those message types that need to be able to
 * evolve and survive cases of "new client sending new stuff" that can
 * occur during upgrades. Implemented by basically collecting unknown
 * property names, if any seen; and caller can determine what to do with
 * those.
 */
public abstract class ExtensibleType
{
    /**
     * For bit of defensive coding, allow crap to be accumulated...
     */
    protected ArrayList<String> _unknown;

    /**
     * To keep things robust, we will allow unknown properties to come in;
     * but store unrecognized ones into set of "unknown" values.
     */
    @JsonAnyGetter
    public void unknownProperty(String key, Object value) {
        if (_unknown == null) {
            _unknown = new ArrayList<String>();
        }
        _unknown.add(key);
    }

    public boolean hasUnknownProperties() {
        return (_unknown != null);
    }

    public List<String> unknownProperties() {
        if (_unknown == null) {
            return Collections.emptyList();
        }
        return _unknown;
    }
}
