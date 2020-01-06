package com.jarcadia.watchdog.deploy;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.rcommando.RedisMap;
import com.jarcadia.rcommando.RedisObject;
import com.jarcadia.retask.Retask;
import com.jarcadia.retask.annontations.RetaskChangeHandler;
import com.jarcadia.retask.annontations.RetaskHandler;
import com.jarcadia.retask.annontations.RetaskParam;
import com.jarcadia.retask.annontations.RetaskWorker;
import com.jarcadia.watchdog.InstanceState;
import com.jarcadia.watchdog.distribute.DistributionState;

@RetaskWorker
public class DeploymentWorker {
    
    private final Logger logger = LoggerFactory.getLogger(DeploymentWorker.class);
    
    private final RedisCommando rcommando;
    private final DeploymentAgent deployAgent;
    
    public DeploymentWorker(RedisCommando rcommando,
            DeploymentAgent deploymentAgent) {
        this.rcommando = rcommando;
        this.deployAgent = deploymentAgent;
    }

    @RetaskHandler("deploy")
    public List<Retask> deploy(RedisMap instances, List<String> groupIds, String version, Path localPath, String hash) {
        String cohortId = UUID.randomUUID().toString();
        
        Map<String, List<String>> instancesByGroup = groupIds.stream()
                .map(groupId -> rcommando.getMap("groups").get(groupId))
                .collect(Collectors.toMap(RedisObject::getId, obj -> obj.get("instances").asListOf(String.class)));

        Map<String, String> instanceToGroupMap = new HashMap<>();
        for (Entry<String, List<String>> entry : instancesByGroup.entrySet()) {
            for (String instance : entry.getValue()) {
                instanceToGroupMap.put(instance, entry.getKey());
            }
        }
        
        Set<String> instanceIds = instanceToGroupMap.keySet();
        
        // Confirm all instances are the same type
        List<String> types = instanceIds.stream()
                .map(id -> instances.get(id).get("type").asString())
                .distinct()
                .collect(Collectors.toList());
        if (types.size() > 1) {
            throw new DeploymentException("Multiple instance types cannot be part of the same deployment: " + types.toString());
        }
        String type = types.get(0);

        // Ensure none of the involved instances are already deploying
        Set<String> alreadyDeploying = rcommando.mergeIntoSetIfDistinct("deploy.active", instanceIds);
        if (alreadyDeploying.size() > 0) {
            throw new DeploymentException(String.format("%s is already part of active deployment", alreadyDeploying.toString()));
        }

        // Prepare the deployments
        RedisMap deployments = rcommando.getMap("deployments");
        List<Retask> tasks = new ArrayList<>();
        for (String groupId : groupIds) {
            RedisObject deployment = deployments.get(UUID.randomUUID().toString());
            List<String> deploymentInstances = instancesByGroup.get(groupId);
            deployment.checkedSet("cohort", cohortId, "group", groupId, "remaining", deploymentInstances);
            tasks.add(Retask.create("deploy.advance").param("deploymentId", deployment.getId()));

            for (String instanceId : deploymentInstances) {
                instances.get(instanceId).checkedSet("deploymentId", deployment.getId(), "deploymentState", DeployState.Waiting);
                rcommando.core().sadd("deploy.pending", instanceId);
            }
        }
        
        Map<String, List<RedisObject>> instancesByHost = instanceIds.stream()
                .map(id -> instances.get(id))
                .collect(Collectors.groupingBy(instance -> instance.get("host").asString()));

        // Prepare the source file
        String localFilename = localPath.getFileName().toString();
        String localExtension = localFilename.substring(localFilename.lastIndexOf("."));
        String remoteFilename = type + "_" + version + "_" + cohortId + localExtension;
        Path remotePath = Paths.get("/tmp", remoteFilename);
        
        // Create a distribution for each host
        RedisMap distributions = rcommando.getMap("distributions");
        for (String host : instancesByHost.keySet()) {
            List<RedisObject> dependentInstances = instancesByHost.get(host);
            List<String> dependentInstanceIds = dependentInstances.stream().map(RedisObject::getId).collect(Collectors.toList());
            RedisObject distribution = distributions.get(UUID.randomUUID().toString());
            distribution.checkedSet("state", DistributionState.PendingTransfer,
                    "type", type,
                    "host", host,
                    "version", version,
                    "localPath", localPath,
                    "hash", hash,
                    "remotePath", remotePath,
                    "dependents", dependentInstanceIds);
            for (RedisObject instance : instancesByHost.get(host)) {
                instance.set("distributionId", distribution.getId());
            }
            tasks.add(Retask.create("distribution.transfer").objParam(distribution));
//                    .param("distributionId", distribution.getId())
//                    .param("host", host)
//                    .param("localPath", localPath)
//                    .param("hash", hash)
//                    .param("remotePath", remotePath));
        }
        return tasks;
    }

    @RetaskHandler("deploy.advance")
    public Retask advanceDeployment(RedisMap instances, @RetaskParam("deployments[deploymentId]") RedisObject deployment) {
        List<String> remainingIds = deployment.get("remaining").asListOf(String.class);
        if (remainingIds.isEmpty()) {
            logger.info("Completed deployment {}", deployment.getId());
            deployment.checkedDelete();
        } else if (remainingIds.size() == 1) {
            RedisObject next = instances.get(remainingIds.get(0));
            next.checkedSet("deploymentState", DeployState.Ready);
        } else {
            List<RedisObject> remaining = remainingIds.stream()
                    .map(id -> instances.get(id))
                    .collect(Collectors.toList());
            deployAgent.chooseNext(deployment, remaining);
        }
        return null;
    }


    @RetaskChangeHandler(mapKey = "instances", field = "state")
    public Retask instanceStateChanged(RedisMap distributions, RedisObject instance, @RetaskParam("after") InstanceState state) {
        if (state == InstanceState.DISABLED && rcommando.core().srem("deploy.pending-disable", instance.getId()) == 1) {
            instance.checkedSet("deploymentState", DeployState.PendingStop);
            return Retask.create("deploy.stop").param("instanceId", instance.getId());
        } else if (state == InstanceState.DOWN && rcommando.core().srem("deploy.pending-stop", instance.getId()) == 1) {
            String distributionId = instance.get("distributionId").asString();
            
            if (distributionId == null) {
                // No distribution to upgrade, just start it
                instance.checkedSet("deploymentState", DeployState.PendingStart);
                return Retask.create("deploy.state").param("instanceId", instance.getId());
            } else {
                // Add ID to pending-distribution set (used to guarantee deploy.upgrade is only invoked once per instance - regardless of whether distribution is finished or not
                rcommando.core().sadd("deploy.pending-distribution", instance.getId());
                RedisObject distribution = distributions.get(instance.get("distributionId").asString());
                
                // If the distribution is already completed
                if (DistributionState.Verified == distribution.get("state").as(DistributionState.class)) {
                    // If instance is still in pending-distribution set, kick off upgrade (handles race condition by ensuring the distribution didn't complete and kick-off upgrade just now)
                    if (rcommando.core().srem("deploy.pending-distribution", instance.getId()) == 1) {
                        instance.checkedSet("deploymentState", DeployState.PendingUpgrade);
                        return Retask.create("deploy.upgrade").param("instanceId", instance.getId());
                    }
                } else {
                    // Distribution is not complete, wait for it (once it completes, the distribution state change will kick off upgrade)
                    instance.checkedSet("deploymentState", DeployState.PendingDistribution);
                }
            }
        } else if (state == InstanceState.DISABLED && rcommando.core().srem("deploy.pending-start", instance.getId()) == 1) {
            instance.checkedSet("deploymentState", DeployState.PendingEnable);
            return Retask.create("deploy.enable").param("instanceId", instance.getId());
        } else if (state == InstanceState.ENABLED && rcommando.core().srem("deploy.pending-enable", instance.getId()) == 1) {
            return Retask.create("deploy.online").param("instanceId", instance.getId());
        }
        return null;
    }

    @RetaskChangeHandler(mapKey = "instances", field = "deploymentState")
    public Retask instanceDeployStateChanged(RedisObject instance, @RetaskParam("after") DeployState deployState) {
        if (deployState == DeployState.Ready && rcommando.core().srem("deploy.pending", instance.getId()) == 1) {
            InstanceState state = instance.get("state").as(InstanceState.class);
            if (state == InstanceState.ENABLED || state == InstanceState.DISABLING) {
                instance.checkedSet("deploymentState", DeployState.PendingDisable);
                return Retask.create("deploy.disable").param("instanceId", instance.getId());
            } else if (state == InstanceState.DISABLED) {
                instance.checkedSet("deploymentState", DeployState.PendingStop);
                return Retask.create("deploy.stop").param("instanceId", instance.getId());
            } else if (state == InstanceState.DOWN) {
                String distributionId = instance.get("distributionId").asString();
                if (distributionId == null) {
                    instance.checkedSet("deploymentState", DeployState.PendingStart);
                    return Retask.create("deploy.start").param("instanceId", instance.getId());
                } else {
                    instance.checkedSet("deploymentState", DeployState.PendingUpgrade);
                    return Retask.create("deploy.upgrade").param("instanceId", instance.getId());
                }
            }
        } else if (deployState == DeployState.Upgraded && rcommando.core().srem("deploy.pending-upgrade", instance.getId()) == 1) {
            instance.checkedSet("deploymentState", DeployState.PendingStart);
            return Retask.create("deploy.start").param("instanceId", instance.getId());
        }
        return null;
    }


    @RetaskHandler("deploy.disable")
    public void disable(@RetaskParam("instances[instanceId]") RedisObject instance) throws Exception {
        instance.checkedSet("deploymentState", DeployState.Disabling);
        rcommando.core().sadd("deploy.pending-disable", instance.getId());
        deployAgent.disable(instance);
    }

    @RetaskHandler("deploy.stop")
    public void stop(@RetaskParam("instances[instanceId]") RedisObject instance) throws Exception {
        instance.checkedSet("deploymentState", DeployState.Stopping);
        rcommando.core().sadd("deploy.pending-stop", instance.getId());
        deployAgent.stop(instance);
    }

    @RetaskHandler( "deploy.upgrade")
    public Retask upgrade(RedisMap distributions, @RetaskParam("instances[instanceId]") RedisObject instance) {
        logger.info("Upgrading {}", instance.getId());
        instance.checkedSet("deploymentState", DeployState.Upgrading);
        RedisObject distribution = distributions.get(instance.getId());
        try {
            deployAgent.upgrade(instance, distribution);
            instance.checkedSet("deploymentState", DeployState.PendingStart);
            return Retask.create("deploy.start").param("instanceId", instance.getId());
        } catch (Exception ex) {
            handleAgentFailure(instance, ex);
            return null;
        }
    }

    @RetaskHandler("deploy.start")
    public void start(@RetaskParam("instances[instanceId]") RedisObject instance) throws Exception {
        instance.checkedSet("deploymentState", DeployState.Starting);
        rcommando.core().sadd("deploy.pending-start", instance.getId());
        deployAgent.start(instance);
    }

    @RetaskHandler("deploy.enable")
    public void enable(@RetaskParam("instances[instanceId]") RedisObject instance) throws Exception {
        instance.checkedSet("deploymentState", DeployState.Enabling);
        rcommando.core().sadd("deploy.pending-enable", instance.getId());
        deployAgent.enable(instance);
    }

    @RetaskHandler("deploy.online")
    public Retask online(@RetaskParam("instances[instanceId]") RedisObject instance, RedisMap deployments) throws IOException {
        String deploymentId = instance.get("deploymentId").asString();
        RedisObject deployment = deployments.get(deploymentId);
        List<String> remaining = deployment.get("remaining").asListOf(String.class).stream()
                .filter(id -> !instance.getId().equals(id))
                .collect(Collectors.toList());
        deployment.checkedSet("remaining", remaining);
        logger.info("Completed instance {} in deployment {}", instance.getId(), deploymentId);
        return Retask.create("deploy.advance").param("deploymentId", deploymentId);
    }

    private void handleAgentFailure(RedisObject instance, Exception ex) {
        StringWriter stackTraceWriter = new StringWriter();
        ex.printStackTrace(new PrintWriter(stackTraceWriter));
        instance.checkedSet("deploymentState", DeployState.Failed, "deploymentError", stackTraceWriter.toString());
    }
}
