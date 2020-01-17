package com.jarcadia.watchdog;

import java.util.List;
import java.util.Set;

import com.jarcadia.rcommando.RedisObject;

public interface DiscoveryAgent {
    
    public Set<DiscoveredInstance> discoverInstances() throws Exception;
    
    public Set<DiscoveredGroup> groupInstances(String type, List<RedisObject> instances);
    
    public Set<DiscoveredArtifact> discoverArtifacts();

}
