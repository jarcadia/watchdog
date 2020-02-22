package com.jarcadia.watchdog.model;

import com.jarcadia.rcommando.proxy.DaoProxy;

public interface Artifact extends DaoProxy {
	
	public String getApp();
	public String getVersion();
}
