package com.jarcadia.watchdog;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jarcadia.rcommando.RedisCdl;
import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.rcommando.RedisMap;
import com.jarcadia.rcommando.RedisObject;
import com.jarcadia.retask.Retask;
import com.jarcadia.retask.annontations.RetaskChangeHandler;
import com.jarcadia.retask.annontations.RetaskHandler;
import com.jarcadia.retask.annontations.RetaskParam;
import com.jarcadia.retask.annontations.RetaskWorker;

@RetaskWorker
public class DeploymentWorker {

    private final Logger logger = LoggerFactory.getLogger(DeploymentWorker.class);

    private final RedisCommando rcommando;
    private final DeploymentAgent deployAgent;
    private final RedisMap instancesMap;
    private final RedisMap deploymentsMap;
    private final RedisMap distributionsMap;
    private final RedisMap binariesMap;

    protected DeploymentWorker(RedisCommando rcommando,
            DeploymentAgent deploymentAgent) {
        this.rcommando = rcommando;
        this.deployAgent = deploymentAgent;
        this.instancesMap = rcommando.getMap("instances");
        this.deploymentsMap = rcommando.getMap("deployments");
        this.distributionsMap = rcommando.getMap("distributions");
        this.binariesMap = rcommando.getMap("binaries");
    }

    @RetaskHandler("deploy")
    public List<Retask> deploy(String binaryId, Set<String> instanceIds) {
        String cohortId = UUID.randomUUID().toString();
        List<RedisObject> deployInstances = instanceIds.stream()
                .map(id -> instancesMap.get(id))
                .collect(Collectors.toList());
        
        // Confirm all instances are the same type
        List<String> types = deployInstances.stream()
                .map(i -> i.get("type").asString())
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

        // Group the instances, according to their groupId or their ID if not part of a group
        Map<String, List<RedisObject>> instancesByGroup = deployInstances.stream()
                .collect(Collectors.groupingBy(i -> i.get("group").isNull() ? i.getId() : i.get("group").asString()));

        // Prepare the deployments
        List<Retask> tasks = new LinkedList<>();
        for (List<RedisObject> groupInstances : instancesByGroup.values()) {
            RedisObject deployment = deploymentsMap.get(UUID.randomUUID().toString());
            List<String> deploymentInstanceIds = groupInstances.stream().map(RedisObject::getId).collect(Collectors.toList());
            deployment.checkedSet("cohort", cohortId, "instances", deploymentInstanceIds, "remaining", deploymentInstanceIds);
            Retask.create("deploy.advance").objParam(deployment).addTo(tasks);

            for (RedisObject instance : groupInstances) {
                instance.checkedSet("deploymentId", deployment.getId(), "deploymentState", DeployState.Waiting);
                setAdd("deploy.pending", instance);
            }
        }

        // Group the deployment instances by host 
        Map<String, List<RedisObject>> instancesByHost = deployInstances.stream()
                .collect(Collectors.groupingBy(instance -> instance.get("host").asString()));

        // Create a distribution for each host
        for (String host : instancesByHost.keySet()) {
            List<RedisObject> dependentInstances = instancesByHost.get(host);
            List<String> dependentInstanceIds = dependentInstances.stream().map(RedisObject::getId).collect(Collectors.toList());
            RedisObject distribution = distributionsMap.get(UUID.randomUUID().toString());
            distribution.checkedSet("state", DistributionState.PendingTransfer,
                    "type", type,
                    "host", host,
                    "binaryId", binaryId,
                    "dependents", dependentInstanceIds);
            Retask.create("distribution.transfer")
                .objParam("distribution", distribution)
                .objParam("binary", "binaries", binaryId)
                .param("host", host)
                .addTo(tasks);

            // Setup cleanup CDL for the distribution
            rcommando.getCdl(distribution.getId() + ".cleanup").init(dependentInstances.size());

            // Set the distribution ID for each instance
            for (RedisObject instance : instancesByHost.get(host)) {
                instance.checkedSet("distributionId", distribution.getId());
            }

        }
        return tasks;
    }

    @RetaskHandler("deploy.advance")
    public Retask advanceDeployment(RedisObject deployment) {
        List<RedisObject> remainingInstances = deployment.get("remaining").asObjectList(instancesMap);
        if (remainingInstances.isEmpty()) {
            completedDeploymentHandler(deployment);
        } else if (remainingInstances.size() == 1) {
            remainingInstances.get(0).checkedSet("deploymentState", DeployState.Ready);
        } else {
            deployAgent.chooseNext(deployment, remainingInstances);
        }
        return null;
    }
    
    private void completedDeploymentHandler(RedisObject deployment) {
        // Update deploy state and remove instances from deploy.active
        List<RedisObject> deploymentInstances = deployment.get("instances").asObjectList(instancesMap);
        for (RedisObject instance : deploymentInstances) {
            instance.checkedClear("deploymentState");
            setRemove("deploy.active", instance);
        }
        deployment.checkedDelete();
        logger.info("Completed deployment {}", deployment.getId());
    }

    @RetaskChangeHandler(mapKey = "instances", field = "deploymentState")
    public List<Retask> instanceDeployStateChanged(RedisObject instance, @RetaskParam("after") DeployState deployState) {
        if (deployState != null) {
            switch (deployState) {
                case Ready:
                    return instanceIsReadyHandler(instance).asList();
                case Upgraded:
                    return instanceIsUpgradedHandler(instance);
                default:
                    return null;
            }
        } else {
            return null;
        }
    }

    private Retask instanceIsReadyHandler(RedisObject instance) {
        if (setRemove("deploy.pending", instance)) {
            InstanceState state = instance.get("state").as(InstanceState.class);
            switch (state) {
                case ENABLED:
                case DISABLING:
                case ENABLING:
                    instance.checkedSet("deploymentState", DeployState.PendingDisable);
                    return Retask.create("deploy.disable").objParam(instance);
                case DISABLED:
                    instance.checkedSet("deploymentState", DeployState.PendingStop);
                    return Retask.create("deploy.stop").objParam(instance);
                case DOWN:
                    Optional<RedisObject> distribution = getInstanceDistribution(instance);
                    if (distribution.isPresent()) {
                        return instanceIsReadyForUpgradeHandler(instance, distribution.get());
                    } else {
                        logger.info("No distribution, going directly to starting");
                        instance.checkedSet("deploymentState", DeployState.PendingStart);
                        return Retask.create("deploy.start").objParam(instance);
                    }
                default:
                    logger.warn("Unexpected instance state {} for instance {}", state, instance.getId());
                    return null;
            }
        } else {
            return null;
        }
    }

    private List<Retask> instanceIsUpgradedHandler(RedisObject instance) {
        if (setRemove("deploy.pending-upgrade", instance)) {
            logger.info("Successfully upgraded {}", instance.getId());
            instance.checkedSet("deploymentState", DeployState.PendingStart);
            Retask startTask = Retask.create("deploy.start").objParam(instance);
            Optional<RedisObject> distribution = getInstanceDistribution(instance);
            if (distribution.isPresent()) {
                String distributionId = distribution.get().getId();
                RedisCdl cleanupCdl = rcommando.getCdl(distributionId + ".cleanup");
                if (cleanupCdl.decrement()) {
                    logger.info("Cleaning up distribution {}", distributionId);
                    return startTask.and(Retask.create("distribution.cleanup").objParam(distribution.get()));
                } else {
                    logger.debug("Other instances still need {}", distributionId);
                }
            } else {
                logger.warn("No distributionId available for instance {}", instance.getId());

            }
            return startTask.asList();
        } else {
            return null;
        }
    }

    @RetaskChangeHandler(mapKey = "instances", field = "state")
    public Retask instanceStateChanged(RedisObject instance, @RetaskParam("after") InstanceState state) {
        switch (state) {
            case DISABLED:
                return instanceIsDisabledHandler(instance);
            case DOWN:
                return instanceIsDownHandler(instance);
            case ENABLED:
                return instanceIsEnabledHandler(instance);
            default:
                return null;
        }
    }

    private Retask instanceIsDisabledHandler(RedisObject instance) {
        if (setRemove("deploy.pending-disable", instance)) {
            instance.checkedSet("deploymentState", DeployState.PendingStop);
            return Retask.create("deploy.stop").objParam(instance);
        } else if (setRemove("deploy.pending-start", instance)) {
            instance.checkedSet("deploymentState", DeployState.PendingEnable);
            return Retask.create("deploy.enable").objParam(instance);
        } else {
            return null;
        }
    }

    private Retask instanceIsDownHandler(RedisObject instance) {
        if (setRemove("deploy.pending-stop", instance)) {
            Optional<RedisObject> distribution = getInstanceDistribution(instance);
            if (distribution.isPresent()) {
                return instanceIsReadyForUpgradeHandler(instance, distribution.get());
            } else {
                logger.info("No distribution present, just starting it now");
                instance.checkedSet("deploymentState", DeployState.PendingStart);
                return Retask.create("deploy.start").objParam(instance);
            }
        } else {
            return null;
        }
    }

    private Retask instanceIsEnabledHandler(RedisObject instance) {
        if (setRemove("deploy.pending-enable", instance)) {
            instance.checkedSet("deploymentState", DeployState.Online);
            return Retask.create("deploy.online").objParam(instance);
        } else {
            return null;
        }
    }


    @RetaskHandler("deploy.disable")
    public void disable(RedisObject instance) throws Exception {
        logger.info("Disabling {}", instance.getId());
        instance.checkedSet("deploymentState", DeployState.Disabling);
        rcommando.core().sadd("deploy.pending-disable", instance.getId());
        deployAgent.disable(instance);
    }

    @RetaskHandler("deploy.stop")
    public void stop(RedisObject instance) throws Exception {
        logger.info("Stopping {}", instance.getId());
        instance.checkedSet("deploymentState", DeployState.Stopping);
        rcommando.core().sadd("deploy.pending-stop", instance.getId());
        deployAgent.stop(instance);
    }

    @RetaskHandler( "deploy.upgrade")
    public void upgrade(RedisObject instance) {
        logger.info("Upgrading {}", instance.getId());
        Optional<RedisObject> distribution = getInstanceDistribution(instance);
        rcommando.core().sadd("deploy.pending-upgrade", instance.getId());
        instance.checkedSet("deploymentState", DeployState.Upgrading);
        deployAgent.upgrade(instance, distribution.get());
    }

    @RetaskHandler("deploy.start")
    public void start(RedisObject instance) throws Exception {
        logger.info("Starting {}", instance.getId());
        instance.checkedSet("deploymentState", DeployState.Starting);
        rcommando.core().sadd("deploy.pending-start", instance.getId());
        deployAgent.start(instance);
    }

    @RetaskHandler("deploy.enable")
    public void enable(RedisObject instance) throws Exception {
        logger.info("Enabling {}", instance.getId());
        instance.checkedSet("deploymentState", DeployState.Enabling);
        rcommando.core().sadd("deploy.pending-enable", instance.getId());
        deployAgent.enable(instance);
    }

    @RetaskHandler("deploy.online")
    public Retask online(RedisObject instance) throws IOException {
        String deploymentId = instance.get("deploymentId").asString();
        RedisObject deployment = deploymentsMap.get(deploymentId);
        List<String> remaining = deployment.get("remaining").asListOf(String.class).stream()
                .filter(id -> !instance.getId().equals(id))
                .collect(Collectors.toList());
        deployment.checkedSet("remaining", remaining);
        logger.info("Completed instance {} in deployment {}", instance.getId(), deploymentId);
        return Retask.create("deploy.advance").objParam(deployment);
    }

    /*
     * This helper method is invoked when a instance is ready to be upgrade and handles two cases.
     * 
     * 1. The distribution is already completed (state=Verified) and the upgrade can proceed
     * 2. The distribution is not complete, the dependent instance is set to PendingDistribution
     * 
     * In the second case, the @RetaskChangeHandler for the distribution.state will advance the instance when the distribution
     * state transitions to Verified.
     * 
     * The redis pending-distribution set is used in both cases to help ensure that the race condition cannot cause both methods to occur
     */
    private Retask instanceIsReadyForUpgradeHandler(RedisObject instance, RedisObject distribution) {
        logger.info("Verifying distribution {} before upgrade on {}", distribution.getId(), instance.getId());
        // Add ID to pending-distribution set (used to guarantee deploy.upgrade is only invoked once per instance - regardless of whether distribution is finished or not
        rcommando.core().sadd("deploy.pending-distribution", instance.getId());

        if (DistributionState.Transferred == distribution.get("state").as(DistributionState.class)) {
            // Distribution is complete
            if (rcommando.core().srem("deploy.pending-distribution", instance.getId()) == 1) {
                logger.info("Distribution was verified, upgrading instance");
                instance.checkedSet("deploymentState", DeployState.PendingUpgrade);
                return Retask.create("deploy.upgrade").objParam(instance);
            } else {
                logger.warn("Unlikely race condition occurred on instance {}, instance is expected to have been advanced by distribution {} state -> Verified", instance.getId(), distribution.getId());
                return null;
            }
        } else {
            // Distribution is not complete, set instance to PendingDistribution
            logger.info("Waiting for distribution {} to complete before advancing {}", distribution.getId(), instance.getId());
            instance.checkedSet("deploymentState", DeployState.PendingDistribution);
            return null;
        }
    }

    private Optional<RedisObject> getInstanceDistribution(RedisObject instance) {
        Optional<String> distributionId = instance.get("distributionId").asOptionalString();
        if (distributionId.isPresent()) {
            return Optional.of(distributionsMap.get(distributionId.get()));
        } else {
            return Optional.empty();
        }
    }
    
    private void setAdd(String set, RedisObject instance) {
        rcommando.core().sadd(set, instance.getId());
    }

    private boolean setRemove(String set, RedisObject instance) {
        return rcommando.core().srem(set, instance.getId()) == 1;
    }

}
