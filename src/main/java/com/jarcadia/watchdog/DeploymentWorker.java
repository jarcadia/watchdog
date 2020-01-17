package com.jarcadia.watchdog;

import java.io.IOException;
import java.util.Arrays;
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
import com.jarcadia.rcommando.RedisValue;
import com.jarcadia.rcommando.RedisValues;
import com.jarcadia.retask.Retask;
import com.jarcadia.retask.RetaskService;
import com.jarcadia.retask.annontations.RetaskChangeHandler;
import com.jarcadia.retask.annontations.RetaskHandler;
import com.jarcadia.retask.annontations.RetaskParam;
import com.jarcadia.retask.annontations.RetaskWorker;

@RetaskWorker
public class DeploymentWorker {

    private final Logger logger = LoggerFactory.getLogger(DeploymentWorker.class);

    private final RedisCommando rcommando;
    private final RetaskService retaskService;
    private final RedisMap instancesMap;
    private final RedisMap deploymentsMap;
    private final RedisMap artifactsMap;
    private final RedisMap distributionsMap;

    protected DeploymentWorker(RedisCommando rcommando,
            RetaskService retaskService) {
        this.rcommando = rcommando;
        this.retaskService = retaskService;
        this.instancesMap = rcommando.getMap("instances");
        this.deploymentsMap = rcommando.getMap("deployments");
        this.artifactsMap = rcommando.getMap("artifacts");
        this.distributionsMap = rcommando.getMap("distributions");
    }

    @RetaskHandler("deploy")
    public List<Retask> deploy(String artifactId, Set<String> instanceIds) {
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

        // Confirm the artifact exists and the type matches the instances' type
        if (!artifactsMap.has(artifactId)) {
            throw new DeploymentException("Artifact with id " + artifactId + " does not exist");
        }
        RedisObject artifact = artifactsMap.get(artifactId);
        RedisValues artifactValues = artifact.get("type", "version");
        String artifactType = artifactValues.next().asString();
        String artifactVersion = artifactValues.next().asString();
        if (!type.equals(artifactType)) {
            throw new DeploymentException("Artifact type " + artifactType + " for artifact " + artifactId + " must match instance type " + type);
        }

        // Verify all expected routes are handled
        List<String> expectedRoutes = Arrays.asList(
                "deploy.next." + type, "deploy.drain." + type,
                "deploy.stop." + type, "deploy.upgrade." + type,
                "deploy.start." + type, "deploy.enable." + type,
                "deploy.distribute." + type, "deploy.cleanup." + type);
        Set<String> missingRoutes = retaskService.verifyRoutes(expectedRoutes);
        if (!missingRoutes.isEmpty()) {
            throw new DeploymentException("Missing deployment routes: " + missingRoutes.toString());
        }

        // Ensure none of the involved instances are already deploying
        Set<String> alreadyDeploying = rcommando.mergeIntoSetIfDistinct("deploy.active", instanceIds);
        if (alreadyDeploying.size() > 0) {
            throw new DeploymentException(String.format("%s is already part of active deployment", alreadyDeploying.toString()));
        }

        // Group the instances, according to their groupId or their ID if not part of a group
        Map<String, List<RedisObject>> instancesByGroup = deployInstances.stream()
                .collect(Collectors.groupingBy(i -> i.get("group").isPresent() ? i.get("group").asString() : i.getId()));

        // Prepare the deployments
        List<Retask> tasks = new LinkedList<>();
        for (List<RedisObject> groupInstances : instancesByGroup.values()) {
            RedisObject deployment = deploymentsMap.get(UUID.randomUUID().toString());
            List<String> deploymentInstanceIds = groupInstances.stream().map(RedisObject::getId).collect(Collectors.toList());
            deployment.checkedSet("type", type, "cohort", cohortId, "instances", deploymentInstanceIds, "remaining", deploymentInstanceIds);
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
                    "artifactId", artifact.getId(),
                    "version", artifactVersion,
                    "dependents", dependentInstanceIds);

            Retask.create("deploy.distribute")
                .objParam("distribution", distribution)
                .objParam("artifact", artifact)
                .param("host", host)
                .param("type", type)
                .addTo(tasks);

            // Setup cleanup CDL for the distributions
            rcommando.getCdl(distribution.getId() + ".cleanup").init(dependentInstances.size());

            // Set the distributionId for each instance
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
            String type = deployment.get("type").asString();
            return Retask.create("deploy.next." + type)
                    .objParam("deployment", deployment)
                    .param("remaining", remainingInstances.stream().map(RedisObject::getId).collect(Collectors.toList()));
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
    public Object instanceDeployStateChanged(RedisObject instance, @RetaskParam("after") DeployState deployState) {
        if (deployState != null) {
            switch (deployState) {
                case Ready:
                    return instanceIsReadyHandler(instance);
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
                case Draining:
                case Enabled:
                    instance.checkedSet("deploymentState", DeployState.PendingDrain);
                    return Retask.create("deploy.disable").objParam(instance);
                case Disabled:
                    instance.checkedSet("deploymentState", DeployState.PendingStop);
                    return Retask.create("deploy.stop").objParam(instance);
                case Down:
                    Optional<RedisObject> distribution = getDistribution(instance);
                    if (distribution.isPresent()) {
                        return instanceIsReadyForUpgradeHandler(instance, distribution.get());
                    } else {
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
            Optional<RedisObject> distribution = getDistribution(instance);
            if (distribution.isPresent()) {
                String distributionId = distribution.get().getId();
                RedisCdl cleanupCdl = rcommando.getCdl(distributionId + ".cleanup");
                if (cleanupCdl.decrement()) {
                    logger.info("Cleaning up distribution {}", distributionId);
                    distribution.get().checkedSet("state", DistributionState.PendingCleanup);
                    return startTask.and(Retask.create("deploy.cleanup")
                            .objParam(distribution.get()));
                }
            }
            return startTask.asList();
        } else {
            return null;
        }
    }

    @RetaskChangeHandler(mapKey = "instances", field = "state")
    public Retask instanceStateChanged(RedisObject instance, @RetaskParam("after") InstanceState state) {
        switch (state) {
            case Disabled:
                return instanceIsDisabledHandler(instance);
            case Down:
                return instanceIsDownHandler(instance);
            case Enabled:
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
            Optional<RedisObject> distribution = getDistribution(instance);
            if (distribution.isPresent()) {
                return instanceIsReadyForUpgradeHandler(instance, distribution.get());
            } else {
                instance.checkedSet("deploymentState", DeployState.PendingStart);
                return Retask.create("deploy.start").objParam(instance);
            }
        } else {
            return null;
        }
    }

    private Retask instanceIsEnabledHandler(RedisObject instance) {
        if (setRemove("deploy.pending-enable", instance)) {
            String deploymentId = instance.get("deploymentId").asString();
            logger.info("Completed instance {} in deployment {}", instance.getId(), deploymentId);
            RedisObject deployment = deploymentsMap.get(deploymentId);
            List<String> remaining = deployment.get("remaining").asListOf(String.class).stream()
                    .filter(id -> !instance.getId().equals(id))
                    .collect(Collectors.toList());
            deployment.checkedSet("remaining", remaining);
            return Retask.create("deploy.advance").objParam(deployment);
        } else {
            return null;
        }
    }

    @RetaskHandler("deploy.disable")
    public Retask disable(RedisObject instance) throws Exception {
        String type = instance.get("type").asString();
        instance.checkedSet("deploymentState", DeployState.Draining);
        logger.info("Draining {} ({})", instance.getId(), type);
        rcommando.core().sadd("deploy.pending-disable", instance.getId());
        return Retask.create("deploy.drain." + type).objParam(instance);
    }

    @RetaskHandler("deploy.stop")
    public Retask stop(RedisObject instance) throws Exception {
        String type = instance.get("type").asString();
        logger.info("Stopping {} ({})", instance.getId(), type);
        instance.checkedSet("deploymentState", DeployState.Stopping);
        rcommando.core().sadd("deploy.pending-stop", instance.getId());
        return Retask.create("deploy.stop." + type).objParam(instance);
    }

    @RetaskHandler("deploy.upgrade")
    public Retask upgrade(RedisObject instance) {
        RedisValues values = instance.get("type", "host", "distributionId");
        String type = values.next().asString();
        String host = values.next().asString();
        RedisObject distribution = values.next().asRedisObject(distributionsMap);
        RedisObject artifact = distribution.get("artifactId").asRedisObject(artifactsMap);
        
        logger.info("Upgrading {} ({})", instance.getId(), type);

        rcommando.core().sadd("deploy.pending-upgrade", instance.getId());
        instance.checkedSet("deploymentState", DeployState.Upgrading);
        return Retask.create("deploy.upgrade." + type)
                .param("host", host)
                .objParam("instance", instance)
                .objParam("distribution", distribution)
                .objParam("artifact", artifact);
    }

    @RetaskHandler("deploy.start")
    public Retask start(RedisObject instance) throws Exception {
        String type = instance.get("type").asString();
        logger.info("Starting {} ({})", instance.getId(), type);
        instance.checkedSet("deploymentState", DeployState.Starting);
        rcommando.core().sadd("deploy.pending-start", instance.getId());
        return Retask.create("deploy.start." + type).objParam(instance);
    }

    @RetaskHandler("deploy.enable")
    public Retask enable(RedisObject instance) throws Exception {
        String type = instance.get("type").asString();
        logger.info("Enabling {} ({})", instance.getId(), type);
        instance.checkedSet("deploymentState", DeployState.Enabling);
        rcommando.core().sadd("deploy.pending-enable", instance.getId());
        return Retask.create("deploy.enable." + type).objParam(instance);
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
     * This helper method is invoked when a instance is ready to be upgraded and handles two cases.
     * 
     * 1. The distribution is already completed (state=Transferred) and the upgrade can proceed immediately
     * 2. The distribution is not complete, this instance is set to PendingDistribution
     * 
     * In the second case, the @RetaskChangeHandler for the distribution.state will advance the instance when the distribution
     * transitions to Transferred.
     * 
     * The redis pending-distribution set is used in both cases to help ensure that the race condition cannot cause both methods to occur
     */
    private Retask instanceIsReadyForUpgradeHandler(RedisObject instance, RedisObject distribution) {
        logger.info("Checking state of distribution {} before upgrading on {}", distribution.getId(), instance.getId());
        rcommando.core().sadd("deploy.pending-distribution", instance.getId());

        if (DistributionState.Transferred == distribution.get("state").as(DistributionState.class)) {
            // Distribution is complete
            if (rcommando.core().srem("deploy.pending-distribution", instance.getId()) == 1) {
                logger.info("Distribution was complete, upgrading instance");
                instance.checkedSet("deploymentState", DeployState.PendingUpgrade);
                return Retask.create("deploy.upgrade").objParam(instance);
            } else {
                logger.warn("Unlikely race condition occurred on instance {}, instance is expected to have been advanced by distribution {} state -> Verified",
                        instance.getId(), distribution.getId());
                return null;
            }
        } else {
            // Distribution is not complete, set instance to PendingDistribution
            logger.info("Waiting for distribution {} to complete before advancing {}", distribution.getId(), instance.getId());
            instance.checkedSet("deploymentState", DeployState.PendingDistribution);
            return null;
        }
    }

    @RetaskChangeHandler(mapKey = "distributions", field = "state")
    public List<Retask> distributionStateChange(RedisObject distribution, DistributionState state) {
        logger.info("Distribution state change for {} -> {}", distribution.getId(), state);
        if (state == DistributionState.Transferred) {
            Set<String> dependentInstanceIds = distribution.get("dependents").asSetOf(String.class);
            return dependentInstanceIds.stream()
                    .filter(id -> rcommando.core().srem("deploy.pending-distribution", id) == 1)
                    .map(id -> Retask.create("deploy.upgrade").objParam("instances", id))
                    .collect(Collectors.toList());
        } else if (state == DistributionState.CleanedUp) {
            distribution.checkedDelete();
            return null;
        } else {
            return null;
        }
    }

    @RetaskHandler(value = "deploy.distribute")
    public Retask distribute(String type, String host, RedisObject distribution, RedisObject artifact) {
        logger.info("Distributing artifact {} ({}) to {} for distribution {}", type, artifact.getId(), host, distribution.getId());
        distribution.checkedSet("state", DistributionState.Transferring);
        return Retask.create("deploy.distribute." + type)
                .param("host", host)
                .objParam("distribution", distribution)
                .objParam("artifact", artifact);
    }

    @RetaskHandler("deploy.cleanup")
    public Retask cleanup(RedisObject distribution) {
        RedisValues values = distribution.get("type", "host", "artifactId");
        String type = values.next().asString();
        String host = values.next().asString();
        RedisObject artifact = values.next().asRedisObject(artifactsMap);
        logger.info("Cleaning up {} {} on {} for distribution {}", type, artifact.getId(), host, distribution.getId());
        distribution.checkedSet("state", DistributionState.PendingCleanup);
        return Retask.create("deploy.cleanup." + type)
                .param("host", host)
                .objParam("distribution", distribution)
                .objParam("artifact", artifact);
    }

    private Optional<RedisObject> getDistribution(RedisObject instance) {
        RedisValue distributionId = instance.get("distributionId");
        if (distributionId.isPresent()) {
            return Optional.of(distributionId.asRedisObject(distributionsMap));
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
