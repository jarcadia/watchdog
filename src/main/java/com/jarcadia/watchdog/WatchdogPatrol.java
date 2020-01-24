package com.jarcadia.watchdog;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface WatchdogPatrol {
    
    String type();
    long interval();
    TimeUnit unit();
    String[] properties() default {};

}
