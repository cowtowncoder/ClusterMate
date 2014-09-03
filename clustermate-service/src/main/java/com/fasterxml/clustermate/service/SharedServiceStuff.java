package com.fasterxml.clustermate.service;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.fasterxml.jackson.dataformat.smile.SmileParser;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.StoreConfig;
import com.fasterxml.storemate.store.file.FileManager;
import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.api.RequestPathStrategy;
import com.fasterxml.clustermate.api.msg.ListItem;
import com.fasterxml.clustermate.json.ClusterMateTypesModule;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.store.StoredEntryConverter;

/**
 * Since we need to pass lots of shared helper objects this class
 * is used to collect them in one place to make passing of
 * things bit easier.
 * Not very elegant but works and is not overly complex.
 */
public abstract class SharedServiceStuff
{
    /*
    /**********************************************************************
    /* Standard fields
    /**********************************************************************
     */
    
    protected final TimeMaster _timeMaster;

    protected final FileManager _fileManager;
    
    protected final ObjectMapper _jsonMapper, _smileMapper;

    protected final RequestPathStrategy<?> _pathStrategy;

    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    protected SharedServiceStuff(TimeMaster timeMaster, FileManager fileManager,
            RequestPathStrategy<?> pathStrategy)
    {
        _timeMaster = timeMaster;
        _fileManager = fileManager;
        _pathStrategy = pathStrategy;

        /* Ok, JSON/Smile support: need to register type handlers
         * (partly to avoid having to annotate types)
         */
        _jsonMapper = new ObjectMapper();
        // with JSON, don't force numerics
        _jsonMapper.registerModule(new ClusterMateTypesModule(false));
        _jsonMapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        
        SmileFactory sf = new SmileFactory();
        // for our data, sharing names fine, shared values are 'meh', but enable
        sf.enable(SmileGenerator.Feature.CHECK_SHARED_NAMES);
        sf.enable(SmileGenerator.Feature.CHECK_SHARED_STRING_VALUES);

        // and although we don't necessarily embed binary data, if we do, better be raw
        sf.disable(SmileGenerator.Feature.ENCODE_BINARY_AS_7BIT);
        // as to header, trailer: header, absolutely must write and require for reads;
        // trailer: let's not; harmless but useless for our uses
        sf.enable(SmileGenerator.Feature.WRITE_HEADER);
        sf.disable(SmileGenerator.Feature.WRITE_END_MARKER);
        sf.enable(SmileParser.Feature.REQUIRE_HEADER);
        
        ObjectMapper smileMapper = new ObjectMapper(sf);
        // with Smile, numerics make sense:
        smileMapper.registerModule(new ClusterMateTypesModule(true));

        // JAX-RS doesn't quite like if we try to close output...
        smileMapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        _smileMapper = smileMapper;
    }

    /*
    /**********************************************************************
    /* Basic config access
    /**********************************************************************
     */

    public abstract <C extends ServiceConfig> C getServiceConfig();
    
    public abstract StoreConfig getStoreConfig();

    public abstract <K extends EntryKey> EntryKeyConverter<K> getKeyConverter();

    public abstract <K extends EntryKey, E extends StoredEntry<K>, L extends ListItem>
    StoredEntryConverter<K,E,L> getEntryConverter();

    public FileManager getFileManager() {
        return _fileManager;
    }

    public TimeMaster getTimeMaster() {
        return _timeMaster;
    }

    public RequestPathStrategy<?> getPathStrategy() {
        return _pathStrategy;
    }

    /*
    /**********************************************************************
    /* Convenience methods for TimeMaster
    /**********************************************************************
     */

    public long currentTimeMillis() {
        return _timeMaster.currentTimeMillis();
    }

    public void sleep(long waitTimeMsecs) throws InterruptedException {
        _timeMaster.sleep(waitTimeMsecs);
    }
    
    /*
    /**********************************************************************
    /* Data formats
    /**********************************************************************
     */

    public ObjectMapper jsonMapper() {
        return _jsonMapper;
    }
    
    public ObjectReader jsonReader(Class<?> type) {
        return _jsonMapper.reader(type);
    }

    public ObjectReader smileReader(Class<?> type) {
        return _smileMapper.reader(type);
    }

    public ObjectWriter jsonWriter() {
        return _jsonMapper.writer();
    }

    public ObjectWriter jsonWriter(Class<?> type) {
        return _jsonMapper.writerWithType(type);
    }

    public ObjectWriter smileWriter() {
        return _smileMapper.writer();
    }

    public ObjectWriter smileWriter(Class<?> type) {
        return _smileMapper.writerWithType(type);
    }
    
    public <T> T convertValue(Object value, Class<T> targetType) throws IOException {
        return _jsonMapper.convertValue(value, targetType);
    }
    
    /*
    /**********************************************************************
    /* Support for tests
    /**********************************************************************
     */

    /**
     * Separate flag set by unit tests, to help reduce some log pollution
     * during tests.
     *<p>
     * Oh yes, ugly it is. Dark times live in we....
     */
    protected boolean _areWeTesting = false;
    
    public void markAsTest() {
        _areWeTesting = true;
    }

    public boolean isRunningTests() {
        return _areWeTesting;
    }
}
