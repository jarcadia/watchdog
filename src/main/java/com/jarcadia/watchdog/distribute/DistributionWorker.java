package com.jarcadia.watchdog.distribute;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.rcommando.RedisMap;
import com.jarcadia.rcommando.RedisObject;
import com.jarcadia.rcommando.RedisValues;
import com.jarcadia.retask.Retask;
import com.jarcadia.retask.annontations.RetaskChangeHandler;
import com.jarcadia.retask.annontations.RetaskHandler;
import com.jarcadia.retask.annontations.RetaskParam;
import com.jarcadia.retask.annontations.RetaskWorker;

@RetaskWorker
public class DistributionWorker {
    
    private final Logger logger = LoggerFactory.getLogger(DistributionWorker.class);
    
    private final RedisCommando rcommando;
    private final DistributionAgent agent;
    
    public DistributionWorker(RedisCommando rcommando, DistributionAgent agent) {
        this.rcommando = rcommando;
        this.agent = agent;
    }
    
    @RetaskHandler(value = "distribution.transfer")
    public void distribute(RedisObject distribution) throws InterruptedException {
        RedisValues values = distribution.get("localPath", "host", "remotePath");
        Path localPath = values.next().as(Path.class);
        String host = values.next().asString();
        Path remotePath = values.next().as(Path.class);

        logger.info("Transferring {} to {} on {}", localPath, remotePath, host);
        distribution.checkedSet("state", DistributionState.Transferring);
        agent.transfer(distribution, host, localPath, remotePath);
    }

    @RetaskHandler("distribution.verify")
    public void verify(RedisObject distribution) {
        RedisValues values = distribution.get("host", "remotePath", "hash");
        String host = values.next().asString();
        Path remotePath = values.next().as(Path.class);
        String hash = values.next().asString();
        logger.info("Verifying {} on {} against {}", remotePath, host, hash);

        distribution.checkedSet("state", DistributionState.Verifying);
        agent.verify(distribution, host, remotePath, hash);
    }
    
    @RetaskChangeHandler(mapKey = "distributions", field = "state")
    public List<Retask> distributionStateChange(String taskId, RedisObject distribution, @RetaskParam("after") DistributionState state) {
        logger.info("Distribution State change for {} -> {}", distribution.getId(), state);
        if (state == DistributionState.Transferred) {
            distribution.checkedSet("state", DistributionState.PendingVerification);
            return Collections.singletonList(Retask.create("distribution.verify").objParam(distribution));
        }
        if (state == DistributionState.Verified) {
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
