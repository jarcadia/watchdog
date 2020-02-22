package com.jarcadia.watchdog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.jarcadia.rcommando.Dao;

public class DiscoveredGroup {

    private final String id;
    private final List<Dao> instances;
    private final Map<String, Object> props;

    public DiscoveredGroup(String id, List<Dao> instances) {
        this.id = id;
        this.instances = instances;
        this.props = new HashMap<>();
    }
    
    public void addProp(String name, Object value) {
        this.props.put(name, value);
    }

    protected DiscoveredGroup(String id, List<Dao> instances, Map<String, Object> props) {
        this.id = id;
        this.instances = instances;
        this.props = props;
    }
    
    protected String getId() {
		return id;
	}
    
    protected List<Dao> getInstances() {
		return instances;
	}

    protected Map<String, Object> getProps() {
		return props;
	}
}
