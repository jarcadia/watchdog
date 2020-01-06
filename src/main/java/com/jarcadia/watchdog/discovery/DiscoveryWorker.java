package com.jarcadia.watchdog.discovery;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jarcadia.rcommando.RedisObject;
import com.jarcadia.retask.Retask;
import com.jarcadia.retask.annontations.RetaskHandler;
import com.jarcadia.retask.annontations.RetaskWorker;

@RetaskWorker
public class DiscoveryWorker {
    
    private final Logger logger = LoggerFactory.getLogger(DiscoveryWorker.class);

    private final DiscoveryAgent agent;

    public DiscoveryWorker(DiscoveryAgent agent) {
        this.agent = agent;
    }

    @RetaskHandler("discover")
    public void discover() {
        
        logger.info("Starting discovery");
        
        // TODO add atomic boolean lock to only discover one at a time
        
        List<RedisObject> hosts = agent.discoverHosts();
        
        for (RedisObject host : hosts) {
            Retask.create("discover.host").objParam(host);
        }
    }
    
    @RetaskHandler("discover.host")
    public void discoverInstanceOnHost(RedisObject host) {
        logger.info("Discovering instances on {}", host.getId());
        List<RedisObject> instances = agent.discoverInstances(host);
    }

    // Discover hosts,
    // Discover instances on each host,
    // Group instances across hosts

}
