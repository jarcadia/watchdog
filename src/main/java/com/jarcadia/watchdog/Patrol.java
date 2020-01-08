package com.jarcadia.watchdog;

import java.util.concurrent.TimeUnit;

class Patrol {

    private final String type;
    private final String routingKey;
    private final long interval;
    private final TimeUnit unit;

    public Patrol(String type, String routingKey, long interval, TimeUnit unit) {
        this.type = type;
        this.routingKey = routingKey;
        this.interval = interval;
        this.unit = unit;
    }

    public String getType() {
        return type;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public long getInterval() {
        return interval;
    }
    
    public TimeUnit getUnit() {
        return unit;
    }
}