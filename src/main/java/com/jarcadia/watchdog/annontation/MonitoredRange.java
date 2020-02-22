package com.jarcadia.watchdog.annontation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Repeatable(MonitoredRanges.class)
public @interface MonitoredRange {
	
	String field() default "";
	double panic() default Double.NaN;
	double critical() default Double.NaN;
	double warn() default Double.NaN;
	double attention() default Double.NaN;
    
}