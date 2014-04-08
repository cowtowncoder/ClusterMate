package com.fasterxml.clustermate.dw;

import io.dropwizard.Configuration;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.jetty.RequestLogFactory;
import io.dropwizard.logging.AppenderFactory;
import io.dropwizard.server.AbstractServerFactory;
import io.dropwizard.server.DefaultServerFactory;
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
    public DWConfigBase() {
        // Let's try forcing GZIP to be disabled: may not work wrt
        // data-binding (since that'll recreate objects but...)
        overrideGZIPEnabled(false);
    }

    /*
    /**********************************************************************
    /* Additional/abstract accessors
    /**********************************************************************
     */

    public abstract SCONFIG getServiceConfig();

    public int getApplicationPort() {
        return ((HttpConnectorFactory) defaultServerConfig().getApplicationConnectors().get(0)).getPort();
    }

    public int getAdminPort() {
        return ((HttpConnectorFactory) defaultServerConfig().getAdminConnectors().get(0)).getPort();
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
        appConnector().setPort(p);
        return (THIS) this;
    }

    @SuppressWarnings("unchecked")
    public THIS overrideAdminPort(int p) {
        adminConnector().setPort(p);
        return (THIS) this;
    }

    @SuppressWarnings("unchecked")
    public THIS overrideGZIPEnabled(boolean state) {
        serverConfig().getGzipFilterFactory().setEnabled(state);
        return (THIS) this;
    }

    @SuppressWarnings("unchecked")
    public THIS overrideLogLevel(Level minLevel) {
        // !!! TODO !!!
        //config.getLoggingConfiguration().setLevel(Level.WARN);
        return (THIS) this;
    }
    
    @SuppressWarnings("unchecked")
    public THIS setShutdownGracePeriod(Duration d) {
        serverConfig().setShutdownGracePeriod(d);
        return (THIS) this;
    }

    @SuppressWarnings("unchecked")
    public THIS setSyncGracePeriod(TimeSpan t) {
        getServiceConfig().cfgSyncGracePeriod = t;
        return (THIS) this;
    }

    /**
     * Method called to suppress all request logging; usually during testing.
     */
    @SuppressWarnings("unchecked")
    public THIS disableRequestLog() {
        RequestLogFactory reqLog = serverConfig().getRequestLogFactory();
        reqLog.setAppenders(ImmutableList.<AppenderFactory>of());        
        return (THIS) this;
    }

    protected AbstractServerFactory serverConfig() {
        return (AbstractServerFactory) getServerFactory();
    }

    protected DefaultServerFactory defaultServerConfig() {
        return (DefaultServerFactory) getServerFactory();
    }

    protected HttpConnectorFactory appConnector() {
        return (HttpConnectorFactory) defaultServerConfig().getApplicationConnectors().get(0);
    }
    
    protected HttpConnectorFactory adminConnector() {
        return (HttpConnectorFactory) defaultServerConfig().getAdminConnectors().get(0);
    }
}
