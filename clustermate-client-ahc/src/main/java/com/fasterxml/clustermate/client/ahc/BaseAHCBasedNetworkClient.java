package com.fasterxml.clustermate.client.ahc;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.api.RequestPathBuilder;
import com.fasterxml.clustermate.client.EntryAccessors;
import com.fasterxml.clustermate.client.NetworkClient;
import com.fasterxml.clustermate.client.StoreClientConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.storemate.shared.IpAndPort;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.AsyncHttpProvider;

public abstract class BaseAHCBasedNetworkClient<
    K extends EntryKey,
    CONFIG extends StoreClientConfig<K,CONFIG>
>
    extends NetworkClient<K>
{
    protected final AsyncHttpClient _ahc;

    protected final ObjectMapper _mapper;

    protected final CONFIG _config;

    /**
     * The usual constructor to call; configures AHC using standard
     * settings.
     */
    protected BaseAHCBasedNetworkClient(CONFIG config)
    {
        _config = config;
        _mapper = config.getJsonMapper();
        AsyncHttpClientConfig ahcConfig = new AsyncHttpClientConfig.Builder()
            .setCompressionEnabled(false)
            .setFollowRedirects(false)
            .setAllowPoolingConnection(true)
            .setConnectionTimeoutInMs((int)config.getCallConfig().getConnectTimeoutMsecs())
            .setMaximumConnectionsPerHost(5) // default of 2 is too low
            .setMaximumConnectionsTotal(30) // and 10 is bit skimpy too
            .build();

        AsyncHttpProvider prov;
    
        /* 12-Oct-2012, tatu: After numerous attempts to use Grizzly provider,
         *   I give up. That PoS just does not work. So even though Netty code
         *   is ugly as hell at least it does work well enough to... work
         *   (there is that 40msec overhead for PUTs, still)
         */
        final boolean USE_NETTY_PROVIDER = true;
    
        if (USE_NETTY_PROVIDER) {
            prov = new com.ning.http.client.providers.netty.NettyAsyncHttpProvider(ahcConfig);
        } else {
            if (true) throw new UnsupportedOperationException("No Grizzly provider");
    //         prov = new com.ning.http.client.providers.grizzly.GrizzlyAsyncHttpProvider(ahcConfig);
        }
        _ahc = new AsyncHttpClient(prov, ahcConfig);
    }

    /**
     * Alternate constructor to use to use custom configurations of AHC.
     */
    protected BaseAHCBasedNetworkClient(CONFIG config, AsyncHttpClient ahc)
    {
        _config = config;
        _mapper = config.getJsonMapper();
        _ahc = ahc;
    }

    // For Apache HC, following might be useful:
    /*
    protected HttpClientImpl<VKey> _buildHttpClient(StoreClientConfig config)
    {
        return new AHCBasedHttpClientImpl(config, _jsonMapper);
        // If we wanted to use Apache HTTP Core/Client, this would be useful:
        SchemeRegistry schemeRegistry = new SchemeRegistry();
        schemeRegistry.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
        PoolingClientConnectionManager cm = new PoolingClientConnectionManager(schemeRegistry);
        // 2 per host is not enough; total of 20 is bit skimpy so:
        cm.setDefaultMaxPerRoute(5);
        cm.setMaxTotal(50); // 
        BasicHttpParams params = new BasicHttpParams();
        DefaultHttpClient.setDefaultHttpParams(params);
        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
        HttpProtocolParams.setContentCharset(params, "UTF-8");
        HttpConnectionParams.setTcpNoDelay(params, true);
        HttpConnectionParams.setSocketBufferSize(params, 8192);
        HttpConnectionParams.setSoKeepalive(params, true);s
        // let's use same value
                (int) _config.getConnectTimeoutMsecs());
        // find maximum of read call timeouts to use as read timeout
        long readTimeoutMsecs = Math.max(_config.getDeleteCallTimeoutMsecs(),
                _config.getPutCallTimeoutMsecs());
        HttpConnectionParams.setSoTimeout(params, (int) readTimeoutMsecs);

        _blockingHttpClient = new DefaultHttpClient(cm, params);
    }
*/

    /*
    ///////////////////////////////////////////////////////////////////////
    // Standard factory methods
    ///////////////////////////////////////////////////////////////////////
     */

    @Override
    public RequestPathBuilder pathBuilder(IpAndPort server)
    {
        return new AHCPathBuilder(server);
    }
    
    @Override
    public void shutdown() {
        // Should we call 'close()' or 'closeAsynchronously()'?
        // latter spins up a thread; former blocks. Let's block, for now.
        _ahc.close();
    }
    
    @Override
    public EntryAccessors<K> getEntryAccessors() {
        return new AHCEntryAccessors<K>(_config, _ahc);
    }

    @Override
    public EntryKeyConverter<K> getKeyConverter() {
        return _config.getKeyConverter();
    }
}
