package com.jarcadia.watchdog;

import java.util.Collection;

import com.jarcadia.rcommando.RedisObject;

public interface BinaryAgent {

    public Collection<DiscoveredBinary> discoverBinaries();

    public String hash(RedisObject binary);

    public boolean transfer(String host, RedisObject binary, RedisObject distribution);
}
