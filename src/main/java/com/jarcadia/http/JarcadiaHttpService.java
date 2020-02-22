package com.jarcadia.http;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.client.CookieStore;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.protocol.HttpContext;

import com.jarcadia.rcommando.RedisCommando;

public class JarcadiaHttpService {
	
	private final RedisCommando rcommando;
	private final HttpQueryService queryService;
	
	public JarcadiaHttpService(RedisCommando rcommando) {
		this.rcommando = rcommando;
		this.queryService = new HttpQueryService();
	}
	
	public GetRequest get(String url) {
		return new GetRequest(this, url);
	}
	
	public PostRequest postJson(String url) {
		return new PostRequest(this, url);
	}
	
	public FormPostRequest postForm(String url) {
		return new FormPostRequest(this, url);
	}
	
	protected HttpResponse submit(GetRequest req) throws IOException {
		RequestBuilder builder = RequestBuilder.get(req.getUrl());
        return submitHelper(req, builder);
	}
	
	protected HttpResponse submit(PostRequest req) throws IOException {
		RequestBuilder builder = RequestBuilder.post(req.getUrl());
		// Set body entity first which will force any provided parameters to be URL parameters
        if (req.getBody() != null) {
            builder.setEntity(new ByteArrayEntity(req.getBody().getBytes()));
        }
        return submitHelper(req, builder);
	}
	
	protected HttpResponse submit(FormPostRequest req) throws IOException {
		RequestBuilder builder = RequestBuilder.post(req.getUrl());
        return submitHelper(req, builder);
	}
	
	private HttpResponse submitHelper(HttpRequest req, RequestBuilder builder) throws IOException {
		applyHeaders(builder, req.getHeaders());
		applyParameters(builder, req.getParams());
        return queryService.request(builder, createContext(req));
	}
	
	private void applyHeaders(RequestBuilder builder, Map<String, String> headers) {
		if (headers != null) {
            for (Entry<String, String> header : headers.entrySet()) {
                builder.addHeader(header.getKey(), header.getValue());
            }
		}
	}
	
	private void applyParameters(RequestBuilder builder, Map<String, String> params) {
		if (params != null) {
            for (Entry<String, String> header : params.entrySet()) {
                builder.addParameter(header.getKey(), header.getValue());
            }
		}
	}
	
    private HttpContext createContext(HttpRequest req) {
        HttpClientContext context = new HttpClientContext();
        
        if (req.getCookieStoreId() != null) {
            CookieStore cookieStore = new JarcadiaCookieStore(rcommando, req.getCookieStoreId());
            context.setCookieStore(cookieStore);
        }
        
        if (req.isDontFollowRedirects()) {
            context.setAttribute("dontFollowRedirects", true);
        }
        
//            context.set
        
        return context;
    }
}
