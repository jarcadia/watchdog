package com.jarcadia.watchdog;

import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.rcommando.TimeSeries;

public class NotificationService {

    private final TimeSeries notifications;

    public NotificationService(RedisCommando rcommando) {
        this.notifications = rcommando.getTimeSeriesOf("notification");
    }

    public void info(String msg) {
        notifications.insert("level", "info", "ts", System.currentTimeMillis(), "msg", msg);
    }

    public void warn(String msg) {
        notifications.insert("level", "warn", "ts", System.currentTimeMillis(), "msg", msg);
    }

    public void error(String msg) {
        notifications.insert("level", "error", "ts", System.currentTimeMillis(), "msg", msg);
    }

}
