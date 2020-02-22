package com.jarcadia.watchdog.annontation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Repeatable(MonitoredValues.class)
public @interface MonitoredValue {
	
	String field() default "";
	String[] panic() default {};
	String[] critical() default {};
	String[] warn() default {};
	String[] attention() default {};
	boolean ascending() default true;
    
}