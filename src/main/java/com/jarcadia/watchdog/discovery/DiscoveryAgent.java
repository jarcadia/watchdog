package com.jarcadia.watchdog.discovery;

import java.util.List;

import com.jarcadia.rcommando.RedisObject;

public interface DiscoveryAgent {
    
    public List<RedisObject> discoverHosts();
    
    public List<RedisObject> discoverInstances(RedisObject host);
    
    public List<RedisObject> groupInstances(List<RedisObject> instances);

}
