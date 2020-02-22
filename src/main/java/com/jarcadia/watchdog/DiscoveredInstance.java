package com.jarcadia.watchdog;

import java.util.HashMap;
import java.util.Map;

public class DiscoveredInstance {

    private final String app;
    private final String id;
    private final Map<String, Object> props;
    
    public DiscoveredInstance(String app, String id) {
        this.id = id;
        this.app = app;
        this.props = new HashMap<>();
        this.props.put("app", app);
    }

    public void addProp(String name, Object value) {
        this.props.put(name, value);
    }
    
    protected DiscoveredInstance(String app, String id, Map<String, Object> props) {
        this.id = id;
        this.app = app;
        this.props = props;
    }

    protected String getApp() {
		return app;
	}

    protected String getId() {
		return id;
	}

    protected Map<String, Object> getProps() {
		return props;
	}
}
