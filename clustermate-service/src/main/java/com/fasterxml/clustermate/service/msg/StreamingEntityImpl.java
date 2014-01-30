package com.fasterxml.clustermate.service.msg;

import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.databind.ObjectWriter;

public class StreamingEntityImpl
    implements StreamingResponseContent
{
    protected final ObjectWriter _objectWriter;
    protected final Object _entity;

    public StreamingEntityImpl(ObjectWriter objectWriter, Object e)
    {
        _objectWriter = objectWriter;
        _entity = e;
    }

    /**
     * Generally we do not know it: could do buffering if we really
     * cared to check it for smallish responses.
     */
    @Override
    public long getLength() {
        return -1L;
    }

    @Override
    public void writeContent(OutputStream out) throws IOException
    {
        _objectWriter.writeValue(out, _entity);
    }

    @Override
    public boolean hasFile() { return false; }

    @Override
    public boolean inline() { return true; }
}
