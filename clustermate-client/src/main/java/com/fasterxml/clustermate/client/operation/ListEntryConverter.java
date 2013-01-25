package com.fasterxml.clustermate.client.operation;

import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.clustermate.client.call.ContentConverter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import com.fasterxml.storemate.shared.StorableKey;

public abstract class ListEntryConverter
{
    public static IdConverter idConverter(ObjectMapper mapper) {
        return new IdConverter(mapper);
    }
 
    static class IdConverter implements ContentConverter<StorableKey>
    {
        protected final ObjectReader _reader;
        
        public IdConverter(ObjectMapper mapper) {
            _reader = mapper.reader(byte[].class);
        }

        //return ListType.ids;
        
        @Override
        public StorableKey convert(InputStream in) throws IOException {
            byte[] raw = _reader.readValue(in);
            return new StorableKey(raw);
        }

        @Override
        public StorableKey convert(byte[] buffer, int offset, int length)
                throws IOException {
            byte[] raw = _reader.readValue(buffer, offset, length);
            return new StorableKey(raw);
        }
        
    }

    /*
    static class StringConverter implements ContentConverter<String>
    {
        
    }
    */
}
