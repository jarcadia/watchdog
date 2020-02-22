package com.jarcadia.watchdog.model;

import com.jarcadia.watchdog.WatchdogMonitoringFactory.AlarmLevel;

public interface InstanceAlarm extends AppAssignable {
	
	public Instance getInstance();
	public AlarmLevel getLevel();
	public String getField();
}