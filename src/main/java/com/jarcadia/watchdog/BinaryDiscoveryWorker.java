package com.jarcadia.watchdog;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.rcommando.RedisMap;
import com.jarcadia.rcommando.RedisObject;
import com.jarcadia.retask.Retask;
import com.jarcadia.retask.annontations.RetaskHandler;
import com.jarcadia.retask.annontations.RetaskInsertHandler;
import com.jarcadia.retask.annontations.RetaskWorker;

@RetaskWorker
public class BinaryDiscoveryWorker {
    
    private final RedisCommando rcommando;
    private final BinaryAgent binaryAgent;
    private final RedisMap binaryMap;
    
    public BinaryDiscoveryWorker(RedisCommando rcommando, BinaryAgent binaryAgent) {
        this.rcommando = rcommando;
        this.binaryAgent = binaryAgent;
        this.binaryMap = rcommando.getMap("binaries");
    }

    @RetaskHandler("binary.discover")
    public void discoverBinaries() {
        String discoveryId = UUID.randomUUID().toString();
        Collection<DiscoveredBinary> binaries = binaryAgent.discoverBinaries();
        for (DiscoveredBinary discovered : binaries) {
            String id = discovered.getType() + "v" + discovered.getVersion();
            RedisObject binary = binaryMap.get(id);
            binary.checkedSet("type", discovered.getType(), "version", discovered.getVersion(), "discoveryId", discoveryId);
        }
    }

    @RetaskInsertHandler("binary")
    public Retask onBinaryInsert(RedisObject binary) {
        return Retask.create("binary.hash").param("binary", binary);
    }

    @RetaskHandler("binary.hash")
    public void hashBinary(RedisObject binary) {
        String hash = binaryAgent.hash(binary);
        binary.checkedSet("hash", hash);
    }
}
