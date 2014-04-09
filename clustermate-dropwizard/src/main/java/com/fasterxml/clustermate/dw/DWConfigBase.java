package com.fasterxml.clustermate.dw;

import io.dropwizard.Configuration;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.jetty.RequestLogFactory;
import io.dropwizard.logging.AppenderFactory;
import io.dropwizard.server.AbstractServerFactory;
import io.dropwizard.server.DefaultServerFactory;
import io.dropwizard.server.ServerFactory;
import io.dropwizard.server.SimpleServerFactory;
import io.dropwizard.util.Duration;

import org.skife.config.TimeSpan;

import ch.qos.logback.classic.Level;

import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.google.common.collect.ImmutableList;

public abstract class DWConfigBase<
  SCONFIG extends ServiceConfig,
  THIS extends DWConfigBase<SCONFIG, THIS>
>
    extends Configuration
    implements Cloneable
{
    protected String _appContext = "/";

    public DWConfigBase() {
        _ensureDefaults();
    }

    /*
    /**********************************************************************
    /* Overrides, to ensure invariants
    /**********************************************************************
     */

    // Overridden to ensure we have proper defaults. May become problematic
    // if users actually do want to change defaults; but let's worry about
    // that if this becomes issue.
    @Override
    public void setServerFactory(ServerFactory factory) {
        super.setServerFactory(factory);
        _ensureDefaults();
    }
    
    private void _ensureDefaults() {
        SimpleServerFactory simpleSF = simpleServerFactory();
        if (simpleSF != null) {
            simpleSF.setApplicationContextPath(_appContext);
        }
        // and that gzip is not enabled by accident
        overrideGZIPEnabled(false);
    }
    
    /*
    /**********************************************************************
    /* Additional/abstract accessors
    /**********************************************************************
     */

    public abstract SCONFIG getServiceConfig();

    public int getApplicationPort() {
        AbstractServerFactory sf = serverFactory();
        if (sf instanceof SimpleServerFactory) {
            SimpleServerFactory ssf = (SimpleServerFactory) sf;
            return ((HttpConnectorFactory)ssf.getConnector()).getPort();
        }
        if (sf instanceof DefaultServerFactory) {
            DefaultServerFactory dsf = (DefaultServerFactory) sf;
            return ((HttpConnectorFactory)dsf.getApplicationConnectors().get(0)).getPort();
        }
        throw new IllegalStateException("Unrecognized ServerFactory: "+sf.getClass().getName());
    }

    public int getAdminPort() {
        AbstractServerFactory sf = serverFactory();
        if (sf instanceof SimpleServerFactory) {
            SimpleServerFactory ssf = (SimpleServerFactory) sf;
            return ((HttpConnectorFactory)ssf.getConnector()).getPort();
        }
        if (sf instanceof DefaultServerFactory) {
            DefaultServerFactory dsf = (DefaultServerFactory) sf;
            return ((HttpConnectorFactory)dsf.getAdminConnectors().get(0)).getPort();
        }
        throw new IllegalStateException("Unrecognized ServerFactory: "+sf.getClass().getName());
    }    

    /*
    /**********************************************************************
    /* Copying
    /**********************************************************************
     */

    @SuppressWarnings("unchecked")
    @Override
    public THIS clone() {
        try {
            return (THIS) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException("Failed to clone "+getClass().getName()+", WTH?");
        }
    }

    /*
    /**********************************************************************
    /* Additional mutators; needed to work around DW strict access
    /**********************************************************************
     */

    @SuppressWarnings("unchecked")
    public THIS overrideHttpPort(int p) {
        AbstractServerFactory sf = serverFactory();
        if (sf instanceof SimpleServerFactory) {
            SimpleServerFactory ssf = (SimpleServerFactory) sf;
            ((HttpConnectorFactory)ssf.getConnector()).setPort(p);
        } else if (sf instanceof DefaultServerFactory) {
            DefaultServerFactory dsf = (DefaultServerFactory) sf;
            ((HttpConnectorFactory)dsf.getApplicationConnectors().get(0)).setPort(p);
        } else {
            throw new IllegalStateException("Unrecognized ServerFactory: "+sf.getClass().getName());
        }
        return (THIS) this;
    }

    @SuppressWarnings("unchecked")
    public THIS overrideAdminPort(int p) {
        AbstractServerFactory sf = serverFactory();
        if (sf instanceof SimpleServerFactory) {
            // no admin port; just ignore
        } else if (sf instanceof DefaultServerFactory) {
            DefaultServerFactory dsf = (DefaultServerFactory) sf;
            ((HttpConnectorFactory)dsf.getAdminConnectors().get(0)).setPort(p);
        } else {
            throw new IllegalStateException("Unrecognized ServerFactory: "+sf.getClass().getName());
        }
        return (THIS) this;
    }

    @SuppressWarnings("unchecked")
    public THIS overrideGZIPEnabled(boolean state) {
        serverFactory().getGzipFilterFactory().setEnabled(state);
        return (THIS) this;
    }
    
    @SuppressWarnings("unchecked")
    public THIS overrideLogLevel(Level minLevel) {
        // !!! TODO !!!
        //config.getLoggingConfiguration().setLevel(Level.WARN);
        return (THIS) this;
    }
    
    @SuppressWarnings("unchecked")
    public THIS overrideShutdownGracePeriod(Duration d) {
        serverFactory().setShutdownGracePeriod(d);
        return (THIS) this;
    }

    @SuppressWarnings("unchecked")
    public THIS overrideSyncGracePeriod(TimeSpan t) {
        getServiceConfig().cfgSyncGracePeriod = t;
        return (THIS) this;
    }

    @SuppressWarnings("unchecked")
    public THIS overrideApplicationContextPath(String path) {
        _appContext = path;
        _overrideApplicationContextPath(path);
        return (THIS) this;
    }

    /**
     * Method called to suppress all request logging; usually during testing.
     */
    @SuppressWarnings("unchecked")
    public THIS disableRequestLog() {
        RequestLogFactory reqLog = serverFactory().getRequestLogFactory();
        reqLog.setAppenders(ImmutableList.<AppenderFactory>of());        
        return (THIS) this;
    }

    protected AbstractServerFactory serverFactory() {
        return (AbstractServerFactory) getServerFactory();
    }

    protected SimpleServerFactory simpleServerFactory() {
        AbstractServerFactory sf = serverFactory();
        return (sf instanceof SimpleServerFactory) ? (SimpleServerFactory) sf : null;
    }

    private void _overrideApplicationContextPath(String path) {
        SimpleServerFactory simpleSF = simpleServerFactory();
        if (simpleSF != null) {
            simpleSF.setApplicationContextPath(path);
        }
    }
}
