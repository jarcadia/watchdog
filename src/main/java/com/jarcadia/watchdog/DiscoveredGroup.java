package com.jarcadia.watchdog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.jarcadia.rcommando.RcObject;

public class DiscoveredGroup {

    private String type;
    private String id;
    private Map<String, Object> props;
    
    public DiscoveredGroup() { /* Reserved for Jackson */ }

    public DiscoveredGroup(String type, String id, String name, List<RcObject> instances) {
        this.id = id;
        this.type = type;
        this.props = new HashMap<>();
        this.props.put("type", type);
        this.props.put("name", name);
        this.props.put("instances", instances.stream().map(RcObject::getId).collect(Collectors.toList()));
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
