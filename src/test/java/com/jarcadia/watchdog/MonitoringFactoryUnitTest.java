package com.jarcadia.watchdog;

import java.math.BigDecimal;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.jarcadia.watchdog.WatchdogMonitoringFactory.AlarmLevel;
import com.jarcadia.watchdog.WatchdogMonitoringFactory.RangeLookup;

public class MonitoringFactoryUnitTest {
	
	
	@Test
	public void testAscending() {
		RangeLookup lookup = new RangeLookup(true, new BigDecimal("0.9"), new BigDecimal("0.75"), null, new BigDecimal("0.5"));
		
		Assertions.assertNull(lookup.get("-10000"));
		Assertions.assertNull(lookup.get("0.0"));
		Assertions.assertNull(lookup.get("0.49"));
		Assertions.assertEquals(AlarmLevel.ATTENTION, lookup.get("0.5"));
		Assertions.assertEquals(AlarmLevel.ATTENTION, lookup.get("0.6"));
		Assertions.assertEquals(AlarmLevel.ATTENTION, lookup.get("0.74"));
		Assertions.assertEquals(AlarmLevel.ATTENTION, lookup.get("0.74"));
		Assertions.assertEquals(AlarmLevel.CRITICAL, lookup.get("0.75"));
		Assertions.assertEquals(AlarmLevel.CRITICAL, lookup.get("0.89"));
		Assertions.assertEquals(AlarmLevel.PANIC, lookup.get("0.9"));
		Assertions.assertEquals(AlarmLevel.PANIC, lookup.get("1.0"));
		Assertions.assertEquals(AlarmLevel.PANIC, lookup.get("100000"));

	}
	
	@Test
	public void testDescending() {
		RangeLookup lookup = new RangeLookup(false, new BigDecimal("0.05"), null, new BigDecimal("0.15"), new BigDecimal("0.3"));
		
		Assertions.assertNull(lookup.get("10000"));
		Assertions.assertNull(lookup.get("1.0"));
		Assertions.assertNull(lookup.get("0.5"));
		Assertions.assertNull(lookup.get("0.31"));
		Assertions.assertEquals(AlarmLevel.ATTENTION, lookup.get("0.3"));
		Assertions.assertEquals(AlarmLevel.ATTENTION, lookup.get("0.25"));
		Assertions.assertEquals(AlarmLevel.ATTENTION, lookup.get("0.16"));
		Assertions.assertEquals(AlarmLevel.WARN, lookup.get("0.15"));
		Assertions.assertEquals(AlarmLevel.WARN, lookup.get("0.06"));
		Assertions.assertEquals(AlarmLevel.PANIC, lookup.get("0.05"));
		Assertions.assertEquals(AlarmLevel.PANIC, lookup.get("0"));
		Assertions.assertEquals(AlarmLevel.PANIC, lookup.get("-100000"));

	}

}
