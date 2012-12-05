package com.fasterxml.clustermate.jaxrs;

import java.io.IOException;
import java.io.OutputStream;

import javax.ws.rs.core.StreamingOutput;

import com.fasterxml.clustermate.service.OperationDiagnostics;
import com.fasterxml.clustermate.service.msg.StreamingResponseContent;

/**
 * Glue class that turns a {@link StreamingResponseContent} into JAX-RS
 * {@link StreamingOutput} object
 */
public class StreamingContentAsStreamingOutput implements StreamingOutput
{
    protected final StreamingResponseContent _content;

    protected final OperationDiagnostics _metadata;
    
    public StreamingContentAsStreamingOutput(StreamingResponseContent content) {
        this(content, null);
    }
    
    public StreamingContentAsStreamingOutput(StreamingResponseContent content,
            OperationDiagnostics metadata) {
        _content = content;
        _metadata = metadata;
    }
    
    @Override
    public void write(OutputStream output) throws IOException
    {
        _content.writeContent(output);
    }
}
