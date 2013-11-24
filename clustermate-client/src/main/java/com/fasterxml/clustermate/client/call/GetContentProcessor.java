package com.fasterxml.clustermate.client.call;

import java.io.*;

import com.fasterxml.storemate.shared.compress.Compression;

/**
 * Interface used to create {@link GetContentProcessor.Handler}s that are used 
 * for handling of HTTP GET content.
 *
 * @param <T> Type of result returned by handlers
 */
public abstract class GetContentProcessor<T>
{
    /**
     * Method called to create handler instance
     */
    public abstract Handler<T> createHandler();
    
    public static abstract class Handler<T>
    {
        /**
         * Factory method to use for creating a wrapper that exposes this
         * handler as a simple {@link OutputStream}, to allow adapting to
         * input sources that require a stream.
         */
        public OutputStream asStream() {
            return new OutputStreamWrapper(this);
        }
        
        // // // Sequence of stateful calls for success case

        /**
         * Method that is called before any content is produced
         * via {@link #processContent}.
         *<p>
         * Default implementation simply returns true.
         * 
         * @return True, if content is to be produced; false to abort processing
         */
        public boolean startContent(int statusCode, Compression compression)
            throws IOException
        {
            return true;
        }

        /**
         * Method called to indicate that more content is available.
         * This method may be called zero or more times
         * before {@link #completeContentProcessing} is called.
         * 
         * @return True, if more content is to be sent; false to abort processing
         */
        public abstract boolean processContent(byte[] content, int offset, int length) throws IOException;
    
        /**
         * Method called once stateful content processing has been successfully completed,
         * and last chunk of data is being delivered.
         *<p>
         * Note that if this method is called, {@link #contentProcessingFailed}
         * is NOT called.
         */
        public abstract T completeContentProcessing() throws IOException;
        
        // // // Sequence of stateful calls for error case(s)
    
        /**
         * Method called to indicate that content access failed, due to an unhandled
         * {@link Exception} (either thrown by processor itself, or by client).
         * Processor is expected to handle any necessary cleanup.
         *<p>
         * Note that if this method is called, {@link #completeContentProcessing}
         * is NOT called.
         */
        public abstract void contentProcessingFailed(Throwable t);
    }

    /*
    /**********************************************************************
    /* Helper classes
    /**********************************************************************
     */

    public static class OutputStreamWrapper extends OutputStream
    {
        protected final GetContentProcessor.Handler<?> _handler;
        
        public OutputStreamWrapper(GetContentProcessor.Handler<?> h) {
            if (h == null) {
                throw new NullPointerException("Can not pass null handler");
            }
            _handler = h;
        }
        
        @Override public void close() { }
        @Override public void flush() { }

        @Override
        public void write(byte[] b) throws IOException {
            _handler.processContent(b, 0, b.length);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            _handler.processContent(b, off, len);
        }

        @Override
        public void write(int b) throws IOException {
            byte[] bytes = new byte[1];
            bytes[0] = (byte) b;
            _handler.processContent(bytes, 0, 1);
        }
    }
}
