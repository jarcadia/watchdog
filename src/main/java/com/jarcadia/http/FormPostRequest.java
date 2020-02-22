package com.jarcadia.http;

import java.io.IOException;

public class FormPostRequest extends HttpRequest {
    
    private final JarcadiaHttpService httpService;
    
    protected FormPostRequest(JarcadiaHttpService httpService, String url) {
    	super(url);
    	this.httpService = httpService;
    }
    
    @Override
    public HttpResponse submit() throws IOException {
    	return httpService.submit(this);
    }
}
