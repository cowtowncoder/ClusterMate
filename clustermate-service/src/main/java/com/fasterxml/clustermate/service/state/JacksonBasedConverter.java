package com.fasterxml.clustermate.service.state;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

import com.fasterxml.storemate.shared.util.RawEntryConverter;

public class JacksonBasedConverter<T>
    extends RawEntryConverter<T>
{
    protected final ObjectReader _reader;
    protected final ObjectWriter _writer;

    public JacksonBasedConverter(ObjectMapper mapper, Class<T> type)
    {
        _reader = mapper.reader(type);
        _writer = mapper.writerWithType(type);
    }
    
    @Override
    public T fromRaw(byte[] raw, int offset, int length)
            throws IOException {
        return _reader.readValue(raw, offset, length);
    }

    @Override
    public byte[] toRaw(T value) throws IOException {
        return _writer.writeValueAsBytes(value);
    }

}
