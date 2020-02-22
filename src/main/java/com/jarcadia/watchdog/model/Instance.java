package com.jarcadia.watchdog.model;

import java.util.Optional;

import com.jarcadia.watchdog.DeployState;
import com.jarcadia.watchdog.States.InstanceState;

public interface Instance extends AppAssignable {
	
	public String getHost();
	public Optional<Group> getGroup();
	public InstanceState getState();
	public Optional<DeployState> getDeploymentState();
	
	public void setState(InstanceState state);
	
}