package com.jarcadia.watchdog;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.rcommando.RedisObject;
import com.jarcadia.retask.Retask;
import com.jarcadia.retask.annontations.RetaskChangeHandler;
import com.jarcadia.retask.annontations.RetaskHandler;
import com.jarcadia.retask.annontations.RetaskParam;
import com.jarcadia.retask.annontations.RetaskWorker;

@RetaskWorker
public class DistributionWorker {
    
    private final Logger logger = LoggerFactory.getLogger(DistributionWorker.class);
    
    private final RedisCommando rcommando;
    private final BinaryAgent binaryAgent;
    
    protected DistributionWorker(RedisCommando rcommando, BinaryAgent binaryAgent) {
        this.rcommando = rcommando;
        this.binaryAgent = binaryAgent;
    }
    
    @RetaskHandler(value = "distribution.transfer")
    public void distribute(RedisObject distribution, RedisObject binary, String host) throws InterruptedException {
        logger.info("Transferring binary {} to {}", binary.getId(), host);
        distribution.checkedSet("state", DistributionState.Transferring);
        
        boolean sync = binaryAgent.transfer(host, binary, distribution);
        if (sync) {
            distribution.checkedSet("state", DistributionState.Transferred);
        } else {
            // Waiting for eventual Transferred status
        }
    }
//
//    @RetaskHandler("distribution.verify")
//    public void verify(RedisObject distribution) {
//        RedisValues values = distribution.get("host", "remotePath", "hash");
//        String host = values.next().asString();
//        Path remotePath = values.next().as(Path.class);
//        String hash = values.next().asString();
//        logger.info("Verifying {} on {} against {}", remotePath, host, hash);
//
//        distribution.checkedSet("state", DistributionState.Verifying);
////        agent.verify(distribution, host, remotePath, hash);
//    }
    
    @RetaskHandler("distribution.cleanup")
    public void cleanup(RedisObject distribution) {
        logger.info("Cleaning up distribution {}", distribution.getId());
    }
    
    @RetaskChangeHandler(mapKey = "distributions", field = "state")
    public List<Retask> distributionStateChange(String taskId, RedisObject distribution, @RetaskParam("after") DistributionState state) {
        logger.info("Distribution State change for {} -> {}", distribution.getId(), state);
        if (state == DistributionState.Transferred) {
            Set<String> dependentInstanceIds = distribution.get("dependents").asSetOf(String.class);
            return dependentInstanceIds.stream()
                    .filter(id -> rcommando.core().srem("deploy.pending-distribution", id) == 1)
                    .map(id -> Retask.create("deploy.upgrade").param("instanceId", id))
                    .collect(Collectors.toList());
        } else {
            return null;
        }
    }
}
