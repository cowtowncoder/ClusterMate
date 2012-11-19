package com.fasterxml.clustermate.json;

import java.io.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

import com.fasterxml.clustermate.api.ClusterStatusAccessor;
import com.fasterxml.clustermate.api.ClusterStatusMessage;

/**
 * Jackson-based Converter implementation; added in this package
 * since API module does not depend on Jackson.
 */
public class ClusterMessageConverter extends ClusterStatusAccessor.Converter
{
    protected final ObjectReader _reader;
    protected final ObjectWriter _writer;

    public ClusterMessageConverter(ObjectMapper mapper)
    {
        _reader = mapper.reader(ClusterStatusMessage.class);
        _writer = mapper.writerWithType(ClusterStatusMessage.class);
    }
    
    @Override
    public ClusterStatusMessage fromJSON(InputStream in) throws IOException {
        return _reader.readValue(in);
    }
    
    @Override
    public ClusterStatusMessage fromJSON(byte[] msg, int offset, int len) throws IOException {
        return _reader.readValue(msg, offset, len);
    }
    
    @Override
    public void asJSON(ClusterStatusMessage msg, OutputStream out) throws IOException {
        _writer.writeValue(out, msg);
    }

    @Override
    public byte[] asJSONBytes(ClusterStatusMessage msg, OutputStream out) throws IOException {
        return _writer.writeValueAsBytes(msg);
    }

}
