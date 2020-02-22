package com.jarcadia.watchdog.model;

import com.jarcadia.rcommando.proxy.DaoProxy;
import com.jarcadia.watchdog.States.DistributionState;

public interface Distribution extends DaoProxy {
	
	public String getApp();
	public String getHost();
	public Artifact getArtifact();
    public DistributionState getState();

	public void setState(DistributionState state);
	public void setProgress(double progress);

}
