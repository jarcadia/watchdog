package com.jarcadia.watchdog;

import java.util.HashMap;
import java.util.Map;

public class DiscoveredInstance {
    
    private final String type;
    private final String id;
    private final Map<String, Object> props;
    
    public DiscoveredInstance(String type, String id) {
        this.id = id;
        this.type = type;
        this.props = new HashMap<>();
        this.props.put("type", type);
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
