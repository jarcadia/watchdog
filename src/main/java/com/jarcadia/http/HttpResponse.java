package com.jarcadia.http;

public class HttpResponse {

    private final int statusCode;
    private final String content;
    private final String url;
    private final long duration;
    private final String redirectUrl;

    protected HttpResponse(int statusCode, String url, String content, long duration, String redirectUrl) {
        this.statusCode = statusCode;
        this.content = content;
        this.url = url;
        this.duration = duration;
        this.redirectUrl = redirectUrl;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getContent() {
        return content;
    }

    public int getSize() {
        return content.length();
    }

    public String getUrl() {
        return url;
    }

    public long getTime() {
        return duration;
    }
    
    public String getRedirectUrl() {
    	return this.redirectUrl;
    }
}
