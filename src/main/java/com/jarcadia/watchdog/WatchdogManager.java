package com.jarcadia.watchdog;

import com.jarcadia.retask.RetaskManager;
import com.jarcadia.retask.RetaskStartupCallback;
import com.jarcadia.retask.Task;
import com.jarcadia.retask.WorkerProdvider;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class WatchdogManager {

    private final RetaskManager retaskManager;
    private final NotificationService notificationService;

    public WatchdogManager(RetaskManager retaskManager, NotificationService notificationService) {
        this.retaskManager = retaskManager;
        this.notificationService = notificationService;
    }

    public void start(RetaskStartupCallback callback) {
        retaskManager.start(callback);
    }

    public void start(Task task) {
        retaskManager.start(task);
    }

    public void start() {
        retaskManager.start();
    }

    public void shutdown(long timeout, TimeUnit unit) throws TimeoutException {
        retaskManager.shutdown(timeout, unit);
    }

    public void addWorkerProvider(WorkerProdvider provider) {
        retaskManager.addWorkerProvider(provider);
    }

    public NotificationService getNotificationService() {
        return notificationService;
    }
}
