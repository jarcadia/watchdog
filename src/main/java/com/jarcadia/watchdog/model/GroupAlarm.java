package com.jarcadia.watchdog.model;

import com.jarcadia.watchdog.WatchdogMonitoringFactory.AlarmLevel;

public interface GroupAlarm extends AppAssignable {
	
	public Group getGroup();
	public String getType();
	public AlarmLevel getLevel();
	public String getField();
}