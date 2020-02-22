package com.jarcadia.http;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class HttpRequest {
    
    private final String url;
    private String cookieStoreId;
    private final Map<String, String> headers;
    private final Map<String, String> params;
    private boolean dontFollowRedirects;
    
    protected HttpRequest(String url) {
        this.url = url;
        this.headers = new HashMap<>();
        this.params = new HashMap<>();
    }
    
    public HttpRequest header(String header, String value) {
        this.headers.put(header, value);
        return this;
    }

    public HttpRequest param(String param, String value) {
        this.params.put(param, value);
        return this;
    }
    
    public HttpRequest cookieStore(String cookieStoreId) {
    	this.cookieStoreId = cookieStoreId;
    	return this;
    }
    
    public HttpRequest dontFollowRedirects() {
    	this.dontFollowRedirects = true;
    	return this;
    }
    
    protected String getUrl() {
    	return url;
    }
    
    protected String getCookieStoreId() {
    	return cookieStoreId;
    }
    
    protected Map<String, String> getHeaders() {
    	return headers;
    }
    
    protected Map<String, String> getParams() {
    	return params;
    }
    
    protected boolean isDontFollowRedirects() {
    	return dontFollowRedirects;
    }
    
    public abstract HttpResponse submit() throws IOException;
}
