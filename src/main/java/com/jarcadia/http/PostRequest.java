package com.jarcadia.http;

import java.io.IOException;

public class PostRequest extends HttpRequest {
    
    private final JarcadiaHttpService httpService;
    private String body;
    
    protected PostRequest(JarcadiaHttpService httpService, String url) {
    	super(url);
    	this.httpService = httpService;
    }
    
    public PostRequest body(String body) {
        this.body = body;
        return this;
    }
    
    protected String getBody() {
    	return body;
    }

    @Override
    public HttpResponse submit() throws IOException {
    	return httpService.submit(this);
    }
}
