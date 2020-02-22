package com.jarcadia.watchdog;

import java.util.HashMap;
import java.util.Map;

public class DiscoveredArtifact {

    private final String app;
    private final String version;
    private final Map<String, Object> props;

    public DiscoveredArtifact(String app, String version) {
        this.app = app;
        this.version = version;
        this.props = new HashMap<>();
    }

    public void addProp(String name, Object value) {
        this.props.put(name, value);
    }
    
    protected DiscoveredArtifact(String type, String version, Map<String, Object> props) {
        this.app = type;
        this.version = version;
        this.props = props;
    }

    protected String getApp() {
		return app;
	}

    protected String getVersion() {
		return version;
	}

    protected Map<String, Object> getProps() {
        return this.props;
    }
}
