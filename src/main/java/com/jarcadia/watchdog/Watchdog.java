package com.jarcadia.watchdog;

import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.retask.Retask;
import com.jarcadia.retask.RetaskContext;
import com.jarcadia.retask.RetaskManager;
import com.jarcadia.retask.RetaskRecruiter;

import io.lettuce.core.RedisClient;

public class Watchdog {
    
    public static RetaskManager init(RedisClient redisClient, RedisCommando rcommando, RetaskContext context, String packageName) {

    	// Setup recruitment from source package and internal package
        RetaskRecruiter recruiter = new RetaskRecruiter();
        recruiter.registerTaskHandlerAnnontation(WatchdogPatrol.class, (clazz, method, annotation) -> "patrol." + method.getName());
        recruiter.recruitFromPackage(packageName);
        recruiter.recruitFromPackage("com.jarcadia.watchdog");

        // Setup wrapper instance provider
        WatchdogRetaskWorkerInstanceProvider wdInstanceProvider = new WatchdogRetaskWorkerInstanceProvider(context);
        RetaskManager manager = Retask.init(redisClient, rcommando, recruiter, wdInstanceProvider);

        // Setup dispatcher and discovery worker
        PatrolDispatcher dispatcher = new PatrolDispatcher(manager.getInstance(), rcommando.getObjectMapper());
        DiscoveryWorker discoveryWorker = new DiscoveryWorker(dispatcher);
        wdInstanceProvider.setDiscoveryWorker(discoveryWorker);

        // Setup deployment worker
        DeploymentWorker deploymentWorker = new DeploymentWorker(rcommando);
        wdInstanceProvider.setDeploymentWorker(deploymentWorker);

        return manager;
    }
}
