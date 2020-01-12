package com.jarcadia.watchdog;

import java.util.concurrent.atomic.AtomicReference;

import com.jarcadia.retask.RetaskWorkerInstanceProvider;

class WatchdogRetaskWorkerInstanceProvider implements RetaskWorkerInstanceProvider {
    
    private final RetaskWorkerInstanceProvider delegate;
    private final AtomicReference<DiscoveryWorker> discoveryWorker;
    private final AtomicReference<DeploymentWorker> deploymentWorker;
    private final AtomicReference<DistributionWorker> distributionWorker;

    protected WatchdogRetaskWorkerInstanceProvider(RetaskWorkerInstanceProvider delegate) {
        this.delegate = delegate;
        this.discoveryWorker = new AtomicReference<DiscoveryWorker>();
        this.deploymentWorker = new AtomicReference<DeploymentWorker>();
        this.distributionWorker = new AtomicReference<DistributionWorker>();
    }

    protected void setDiscoveryWorker(DiscoveryWorker discoveryWorker) {
        this.discoveryWorker.set(discoveryWorker);
    }

    protected void setDeploymentWorker(DeploymentWorker deploymentWorker) {
        this.deploymentWorker.set(deploymentWorker);
    }

    protected void setDistributionWorker(DistributionWorker distributionWorker) {
        this.distributionWorker.set(distributionWorker);
    }

    @Override
    public Object getInstance(Class<?> clazz) {
        if (DiscoveryWorker.class.equals(clazz)) {
            return discoveryWorker.get();
        } else if (DeploymentWorker.class.equals(clazz)) {
            return deploymentWorker.get();
        } else if (DistributionWorker.class.equals(clazz)) {
            return distributionWorker.get();
        }
        else {
            return delegate.getInstance(clazz);
        }
    }
}
