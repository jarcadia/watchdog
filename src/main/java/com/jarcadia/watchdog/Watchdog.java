package com.jarcadia.watchdog;

import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.retask.RetaskInit;
import com.jarcadia.retask.RetaskRecruiter;
import com.jarcadia.retask.RetaskService;
import com.jarcadia.retask.RetaskWorkerInstanceProvider;

import io.lettuce.core.RedisClient;

public class Watchdog {
    
    public static RetaskService init(RedisClient redisClient, RedisCommando rcommando, RetaskWorkerInstanceProvider instanceProvider, String packageName,
            DiscoveryAgent discoveryAgent) {

        // Setup Retask
        RetaskRecruiter recruiter = new RetaskRecruiter();
        recruiter.recruitFromPackage(packageName);
        recruiter.recruitFromPackage("com.jarcadia.watchdog");
        
        // Setup wrapper instance provider
        WatchdogRetaskWorkerInstanceProvider wdInstanceProvider = new WatchdogRetaskWorkerInstanceProvider(instanceProvider);

        RetaskService retaskService = RetaskInit.init(redisClient, rcommando, recruiter, wdInstanceProvider);

        // Setup dispatcher and discovery worker
        PatrolDispatcher dispatcher = new PatrolDispatcher(retaskService, packageName);
        DiscoveryWorker discoveryWorker = new DiscoveryWorker(discoveryAgent, dispatcher);
        wdInstanceProvider.setDiscoveryWorker(discoveryWorker);
        
        
        retaskService.start();
        return retaskService;
    }
}
