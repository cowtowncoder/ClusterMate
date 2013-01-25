package com.fasterxml.clustermate.api;

import java.util.HashMap;

/**
 * Enumeration of content types that are currently used by ClusterMate(-based)
 * systems.
 */
public enum ContentType
{
    /* NOTE: entries are ordered in decreasing order of precedence, that is,
     * first entries have priority over latter, if multiple matches are found
     * 
     */
    TEXT("text/plain"),
    JSON("application/json"),
    SMILE("application/x-jackson-smile")
    ;

    private final String _primaryMime;
    
    private final String[] _secondaryMimes;
    
    private ContentType(String primaryMime, String... secondaryMimes) {
        _primaryMime = primaryMime;
        _secondaryMimes = secondaryMimes;
    }

    @Override
    public String toString() {
        return _primaryMime;
    }
    
    public static ContentType findType(String type) {
        return Matcher.instance.match(type);
    }

    public boolean isAccepted(String acceptHeader) {
        if (acceptHeader != null) {
            acceptHeader = acceptHeader.trim();
            if (acceptHeader.length() > 0) {
                return _isAccepted(acceptHeader);
            }
        }
        return false;
    }

    protected boolean _isAccepted(String acceptHeader) {
        if (acceptHeader.indexOf(_primaryMime) >= 0) {
            return true;
        }
        for (int i = 0, len = _secondaryMimes.length; i < len; ++i) {
            if (acceptHeader.indexOf(_secondaryMimes[i]) >= 0) {
                return true;
            }
        }
        return false;
    }
    
    public static ContentType findFromAcceptHeader(String acceptHeader) {
        if (acceptHeader != null) {
            acceptHeader = acceptHeader.trim();
            if (acceptHeader.length() > 0) {
                // let's try quick way first:
                ContentType ct0 = Matcher.instance.match(acceptHeader);
                if (ct0 != null) {
                    return ct0;
                }
                // if not, substrings
                for (ContentType ct : ContentType.values()) {
                    if (ct._isAccepted(acceptHeader)) {
                        return ct;
                    }
                }
            }
        }
        return null;
    }
    
    private static final class Matcher {
        public static Matcher instance = new Matcher();

        private final HashMap<String, ContentType> _mapping = new HashMap<String, ContentType>();
        
        private Matcher() {
            for (ContentType ct : ContentType.values()) {
                _mapping.put(ct._primaryMime, ct);
                for (String alias : ct._secondaryMimes) {
                    _mapping.put(alias, ct);
                }
            }
        }

        public ContentType match(String type) {
            return _mapping.get(type);
        }
    }
}
