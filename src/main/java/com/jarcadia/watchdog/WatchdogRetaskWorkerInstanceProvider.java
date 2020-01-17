package com.jarcadia.watchdog;

import java.util.concurrent.atomic.AtomicReference;

import com.jarcadia.retask.RetaskWorkerInstanceProvider;

class WatchdogRetaskWorkerInstanceProvider implements RetaskWorkerInstanceProvider {
    
    private final RetaskWorkerInstanceProvider delegate;
    private final AtomicReference<DiscoveryWorker> discoveryWorker;
    private final AtomicReference<DeploymentWorker> deploymentWorker;

    protected WatchdogRetaskWorkerInstanceProvider(RetaskWorkerInstanceProvider delegate) {
        this.delegate = delegate;
        this.discoveryWorker = new AtomicReference<DiscoveryWorker>();
        this.deploymentWorker = new AtomicReference<DeploymentWorker>();
    }

    protected void setDiscoveryWorker(DiscoveryWorker discoveryWorker) {
        this.discoveryWorker.set(discoveryWorker);
    }

    protected void setDeploymentWorker(DeploymentWorker deploymentWorker) {
        this.deploymentWorker.set(deploymentWorker);
    }

    @Override
    public Object getInstance(Class<?> clazz) {
        if (DiscoveryWorker.class.equals(clazz)) {
            return discoveryWorker.get();
        } else if (DeploymentWorker.class.equals(clazz)) {
            return deploymentWorker.get();
        }
        else {
            return delegate.getInstance(clazz);
        }
    }
}
