package com.fasterxml.clustermate.api;

import com.fasterxml.storemate.shared.IpAndPort;

/**
 * Basic definition of a store node, needed to figure out which store
 * instance(s) to contact for accessing specified entry or entries.
 */
public class NodeDefinition
{
    /**
     * Index in ring is one-based (unlike most things), so that default int value
     * of 0 can be used for "not known".
     */
    public final static int INDEX_UNKNOWN = 0;
    
    protected final IpAndPort _address;

    protected final int _index;

    /**
     * Range of keys that this node will actively manage, i.e. will
     * serve to clients, and get updates on.
     */
    protected final KeyRange _activeKeyRange;

    /**
     * Range of keys that this node will passively manage, i.e. try
     * to get synchronized, but only serve if no active instance
     * is available to do that.
     */
    protected final KeyRange _passiveKeyRange;

    /**
     * Union of active and passive ranges; typically same as
     * passive range. Used for figuring out range of keys a node
     * will try to synchronize values for.
     */
    protected final KeyRange _totalKeyRange;

    public NodeDefinition(IpAndPort address, int index,
            KeyRange activeRange, KeyRange passiveRange)
    {
        _address = address;
        _index = index;
        _activeKeyRange = activeRange;
        _passiveKeyRange = passiveRange;
        _totalKeyRange = activeRange.union(passiveRange);
    }

    public IpAndPort getAddress() { return _address; }
    public int getIndex() { return _index; }
    
    public KeyRange getActiveRange() { return _activeKeyRange; }
    public KeyRange getPassiveRange() { return _passiveKeyRange; }
    public KeyRange getTotalRange() { return _totalKeyRange; }

    @Override
    public String toString()
    {
        return new StringBuilder()
            .append("{node @")
            .append(_address.toString())
            .append("; index: ").append(_index)
            .append("; active: ").append(_activeKeyRange)
            .append(", passive: ").append(_passiveKeyRange)
            .append("}")
            .toString();
    }
}
