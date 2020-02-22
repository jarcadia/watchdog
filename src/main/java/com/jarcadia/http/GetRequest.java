package com.jarcadia.http;

import java.io.IOException;

public class GetRequest extends HttpRequest {
    
    private final JarcadiaHttpService httpService;
    
    protected GetRequest(JarcadiaHttpService httpService, String url) {
    	super(url);
    	this.httpService = httpService;
    }
    
    public HttpResponse submit() throws IOException {
    	return httpService.submit(this);
    }
}
