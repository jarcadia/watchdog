package com.jarcadia.watchdog;

import java.util.concurrent.atomic.AtomicReference;

import com.jarcadia.retask.RetaskWorkerInstanceProvider;

class WatchdogRetaskWorkerInstanceProvider implements RetaskWorkerInstanceProvider {
    
    private final AtomicReference<DiscoveryWorker> discoveryWorker;
    private final RetaskWorkerInstanceProvider delegate;
    
    protected WatchdogRetaskWorkerInstanceProvider(RetaskWorkerInstanceProvider delegate) {
        this.discoveryWorker = new AtomicReference<DiscoveryWorker>();
        this.delegate = delegate;
    }
    
    protected void setDiscoveryWorker(DiscoveryWorker discoveryWorker) {
        this.discoveryWorker.set(discoveryWorker);
    }

    @Override
    public Object getInstance(Class<?> clazz) {
        if (DiscoveryWorker.class.equals(clazz)) {
            return discoveryWorker.get();
        } else {
            return delegate.getInstance(clazz);
        }
    }
}
