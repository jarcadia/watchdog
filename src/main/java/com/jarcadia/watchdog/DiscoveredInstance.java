package com.jarcadia.watchdog;

import java.util.HashMap;
import java.util.Map;

public class DiscoveredInstance {

    private String type;
    private String id;
    private Map<String, Object> props;
    
    public DiscoveredInstance() { /* Reserved for Jackson */ }

    public DiscoveredInstance(String type, String id, String name) {
        this.id = id;
        this.type = type;
        this.props = new HashMap<>();
        this.props.put("type", type);
        this.props.put("name", name);
    }

    public void addProp(String name, Object value) {
        this.props.put(name, value);
    }

    public String getType() {
		return type;
	}

    public void setType(String type) {
		this.type = type;
	}

    public String getId() {
		return id;
	}

    public void setId(String id) {
		this.id = id;
	}

    public Map<String, Object> getProps() {
		return props;
	}

    public void setProps(Map<String, Object> props) {
		this.props = props;
	}
}
