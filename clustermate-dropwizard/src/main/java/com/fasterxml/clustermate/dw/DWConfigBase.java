package com.fasterxml.clustermate.dw;

import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.yammer.dropwizard.config.Configuration;
import com.yammer.dropwizard.config.GzipConfiguration;
import com.yammer.dropwizard.config.HttpConfiguration;
import com.yammer.dropwizard.util.Duration;

public abstract class DWConfigBase<
  SCONFIG extends ServiceConfig,
  THIS extends DWConfigBase<SCONFIG, THIS>
>
    extends Configuration
    implements Cloneable
{
    public DWConfigBase()
    {
        // important: use custom variant, to get better defaults,
        // including disabling of standard GZIP compression
        http = new CustomHttpConfiguration();
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Abstract accessors
    ///////////////////////////////////////////////////////////////////////
     */

    public abstract SCONFIG getServiceConfig();
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Copying
    ///////////////////////////////////////////////////////////////////////
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
    ///////////////////////////////////////////////////////////////////////
    // Additional mutators; needed to work around DW strict access
    ///////////////////////////////////////////////////////////////////////
     */

    @SuppressWarnings("unchecked")
    public THIS overrideHttpPort(int p) {
        ((CustomHttpConfiguration) http).setPort(p);
        return (THIS) this;
    }

    @SuppressWarnings("unchecked")
    public THIS overrideAdminPort(int p) {
        ((CustomHttpConfiguration) http).setAdminPort(p);
        return (THIS) this;
    }

    @SuppressWarnings("unchecked")
    public THIS overrideGZIPEnabled(boolean state) {
        ((CustomHttpConfiguration) http).overrideGZIPEnabled(state);
        return (THIS) this;
    }

    @SuppressWarnings("unchecked")
    public THIS setShutdownGracePeriod(Duration d) {
        ((CustomHttpConfiguration) http).setShutdownGracePeriod(d);
        return (THIS) this;
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Custom override classes
    ///////////////////////////////////////////////////////////////////////
     */
    
    /**
     * Need to sub-class this just to access protected members.
     * By default will disable GZIP compression.
     */
    public static class CustomHttpConfiguration extends HttpConfiguration
    {
        public CustomHttpConfiguration()
        {
            gzip = new CustomGzipConfiguration();
        }
        
        public CustomHttpConfiguration setPort(int p) {
            port = p;
            return this;
        }

        public CustomHttpConfiguration setAdminPort(int p) {
            adminPort = p;
            return this;
        }

        public CustomHttpConfiguration setShutdownGracePeriod(Duration d) {
            shutdownGracePeriod = d;
            return this;
        }
        
        public CustomHttpConfiguration overrideGZIPEnabled(boolean state) {
            ((CustomGzipConfiguration) gzip).setEnabled(state);
            return this;
        }
    }

    /**
     * Value class only needed to allow overriding of DropWizard
     * gzip configuration
     */
    public static class CustomGzipConfiguration extends GzipConfiguration
    {
        public CustomGzipConfiguration() { }

        public void setEnabled(boolean state) {
            enabled = state;
        }
    }    
}
