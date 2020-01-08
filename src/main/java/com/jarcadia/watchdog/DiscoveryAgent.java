package com.jarcadia.watchdog;

import java.util.Collection;
import java.util.List;

import com.jarcadia.rcommando.RedisObject;

public interface DiscoveryAgent {
    
    public Collection<DiscoveredInstance> discover() throws Exception;
    
    public Collection<DiscoveredGroup> groupInstances(String type, List<RedisObject> instances);
}
