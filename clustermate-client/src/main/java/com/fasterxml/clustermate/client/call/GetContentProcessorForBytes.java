package com.fasterxml.clustermate.client.call;

import java.io.IOException;

import com.fasterxml.storemate.shared.util.ByteAggregator;

/**
 * Simple {@link GetContentProcessor} implementation for GETting content
 * and aggregating it in (and returning as) {@link ByteAggregator}.
 */
public class GetContentProcessorForBytes extends GetContentProcessor<ByteAggregator>
{
    @Override public GetContentProcessorForBytes.Handler createHandler() {
        return new Handler();
    }

    /**
     * Simple {@link PutContentProvider} implementation that collects content
     * as bytes, producing a {@link ByteAggregator}.
     */
    public static class Handler extends GetContentProcessor.Handler<ByteAggregator>
    {
        protected  ByteAggregator _bytes;
        
        public Handler() { }

        @Override
        public boolean processContent(byte[] content, int offset, int length)
            throws IOException
        {
            _bytes = ByteAggregator.with(_bytes, content, offset, length);
            // yeah, let's get all there is?
            return true;
        }

        @Override
        public ByteAggregator completeContentProcessing() throws IOException
        {
            if (_bytes == null) {
                _bytes = new ByteAggregator();
            }
            return _bytes;
        }

        @Override
        public void contentProcessingFailed(Throwable cause) { }
    }
}