package com.fasterxml.clustermate.jaxrs;

import java.io.IOException;
import java.io.OutputStream;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import com.fasterxml.jackson.databind.ObjectWriter;

public class StreamingJson implements StreamingOutput
{
    protected final ObjectWriter _writer;
    protected final Object _value;
	
    public StreamingJson(ObjectWriter w, Object value)
    {
        _writer = w;
        _value = value;
    }

    @Override
    public void write(OutputStream out) throws IOException, WebApplicationException
    {
        _writer.writeValue(out, _value);
    }
}
