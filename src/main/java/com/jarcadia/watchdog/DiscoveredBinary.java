package com.jarcadia.watchdog;

import java.nio.file.Path;

public class DiscoveredBinary {

    private final String type;
    private final String version;

    public DiscoveredBinary(String type, String version) {
        this.type = type;
        this.version = version;
    }

    public String getType() {
        return type;
    }

    public String getVersion() {
        return version;
    }
}
