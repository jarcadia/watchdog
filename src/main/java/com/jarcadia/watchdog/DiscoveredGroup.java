package com.jarcadia.watchdog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.jarcadia.rcommando.RedisObject;

public class DiscoveredGroup {

    private final String type;
    private final String id;
    private final Map<String, Object> props;

    public DiscoveredGroup(String type, String id, String name, List<RedisObject> instances) {
        this.id = id;
        this.type = type;
        this.props = new HashMap<>();
        this.props.put("type", type);
        this.props.put("name", name);
        this.props.put("instances", instances.stream().map(RedisObject::getId).collect(Collectors.toList()));
    }
    
    public String getType() {
        return this.type;
    }
    
    public String getId() {
        return this.id;
    }
    
    public void addProp(String name, Object value) {
        this.props.put(name, value);
    }
    
    protected Map<String, Object> getProps() {
        return this.props;
    }
}
