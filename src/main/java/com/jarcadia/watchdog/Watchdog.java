package com.jarcadia.watchdog;

import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.retask.Retask;
import com.jarcadia.retask.RetaskManager;
import com.jarcadia.retask.RetaskRecruiter;

import io.lettuce.core.RedisClient;

public class Watchdog {
    
    public static RetaskManager init(RedisClient redisClient, RedisCommando rcommando, String packageName) {

    	// Setup recruitment from source package and internal package
        RetaskRecruiter recruiter = new RetaskRecruiter();
        recruiter.registerTaskHandlerAnnontation(WatchdogPatrol.class, (clazz, method, annotation) -> "patrol." + method.getName());
        recruiter.recruitFromPackage(packageName);
        recruiter.recruitFromPackage("com.jarcadia.watchdog");

        RetaskManager manager = Retask.init(redisClient, rcommando, recruiter);

        // Setup dispatcher and discovery worker
        PatrolDispatcher dispatcher = new PatrolDispatcher(rcommando.getObjectMapper(), manager.getHandlersByAnnontation(WatchdogPatrol.class));
        DiscoveryWorker discoveryWorker = new DiscoveryWorker(dispatcher);

        // Setup deployment worker
        DeploymentWorker deploymentWorker = new DeploymentWorker(rcommando);
        manager.addInstances(discoveryWorker, deploymentWorker);
        return manager;
    }
}
