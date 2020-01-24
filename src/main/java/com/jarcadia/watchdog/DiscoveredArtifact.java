package com.jarcadia.watchdog;

import java.util.HashMap;
import java.util.Map;

public class DiscoveredArtifact {

    private String id;
    private String type;
    private String version;
    private Map<String, Object> props;

    public DiscoveredArtifact(String type, String version) {
        this.type = type;
        this.version = version;
        this.id = type + '_' + version;
        this.props = new HashMap<>();
        this.props.put("type", type);
        this.props.put("version", version);
    }

    public void addProp(String name, Object value) {
        this.props.put(name, value);
    }

    public DiscoveredArtifact() { /* Reserved for Jackson */ }
    
    public String getId() {
		return id;
	}

    public void setId(String id) {
		this.id = id;
	}

    public String getType() {
		return type;
	}

    public void setType(String type) {
		this.type = type;
	}

    public String getVersion() {
		return version;
	}

    public void setVersion(String version) {
		this.version = version;
	}

    public void setProps(Map<String, Object> props) {
		this.props = props;
	}

    public Map<String, Object> getProps() {
        return this.props;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        result = prime * result + ((version == null) ? 0 : version.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DiscoveredArtifact other = (DiscoveredArtifact) obj;
        if (type == null) {
            if (other.type != null)
                return false;
        } else if (!type.equals(other.type))
            return false;
        if (version == null) {
            if (other.version != null)
                return false;
        } else if (!version.equals(other.version))
            return false;
        return true;
    }
}
