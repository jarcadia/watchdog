package com.jarcadia.watchdog.model;

import com.jarcadia.rcommando.proxy.Proxy;

public interface Artifact extends Proxy {
	
	public String getApp();
	public String getVersion();
}
