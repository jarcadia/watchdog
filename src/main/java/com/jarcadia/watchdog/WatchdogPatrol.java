package com.jarcadia.watchdog;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface WatchdogPatrol {
    
    String mapKey();
    String routingKey();
    long interval();

}
