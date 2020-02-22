package com.jarcadia.http;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.http.HttpRequest;
import org.apache.http.ProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.DnsResolver;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.impl.client.RedirectLocations;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.conn.SystemDefaultDnsResolver;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpQueryService  {
    private final Logger logger = LoggerFactory.getLogger(HttpQueryService.class);

    private final HttpClient client;

    public HttpQueryService() {
    	int connectTimeout = 5000;

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectTimeout)
                .setConnectionRequestTimeout(connectTimeout)
                .setSocketTimeout(connectTimeout).build();

        DnsResolver customResolver = new SystemDefaultDnsResolver() {
            @Override
            public InetAddress[] resolve(final String host) throws UnknownHostException {
//                Optional<InetAddress> resolved = dnsCacheService.resolve(host);
//                if (resolved.isPresent()) {
//                    logger.debug("Resolving {} from cache", host);
//                    return new InetAddress[]{resolved.get()};
//                } else {
//                    logger.debug("Resolving {} using system default DNS resolver", host);
                    return super.resolve(host);
//                }
            }
        };

        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager(
                RegistryBuilder.<ConnectionSocketFactory>create()
                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                .register("https", SSLConnectionSocketFactory.getSocketFactory())
                .build(),
                customResolver
                );

        this.client = HttpClients.custom()
                .setConnectionManager(connManager)
                .setRedirectStrategy(new PolyRedirectStrategy())
//                .disableRedirectHandling()
                .setDefaultRequestConfig(requestConfig)
                .build();
    }
    
    public HttpResponse request(RequestBuilder builder, HttpContext context) throws IOException {
        long start = System.currentTimeMillis();
        HttpUriRequest request = builder.build();
        org.apache.http.HttpResponse response = client.execute(request, context);
        long duration = System.currentTimeMillis() - start;
        int statusCode = response.getStatusLine().getStatusCode();
        String content = this.readPageContent(response);
        String redirectUrl = (String) context.getAttribute("redirectUrl");
        return new HttpResponse(statusCode, request.getURI().toString(), content, duration, redirectUrl);
    }
    

    
    private String readPageContent(org.apache.http.HttpResponse response) throws UnsupportedOperationException, IOException {
        StringBuilder sbPageContent = new StringBuilder();
        if (response.getEntity() == null) {
            return "";
        }

        InputStream instreamPageContent = response.getEntity().getContent();
        String line;
        try (BufferedReader brPageContent = new BufferedReader(new InputStreamReader(instreamPageContent))) {
            while ((line = brPageContent.readLine()) != null) {
                sbPageContent.append(line);
            }
        }
        return sbPageContent.toString();
    }
    

    public class PolyRedirectStrategy extends LaxRedirectStrategy {

        @Override
        public boolean isRedirected(HttpRequest request, org.apache.http.HttpResponse response, HttpContext context) throws ProtocolException {
            Object attr = context.getAttribute("dontFollowRedirects");
            Boolean dontFollow = attr == null ? false : (Boolean) attr;
            if (dontFollow != null && dontFollow == true) {
            	if (super.isRedirected(request, response, context)) {
            		HttpUriRequest redirect = super.getRedirect(request, response, context);
            		context.setAttribute("redirectUrl", redirect.getURI().toString());
            	}
            	return false;
            } else {
                return super.isRedirected(request, response, context);
            }
        }
    }
}
