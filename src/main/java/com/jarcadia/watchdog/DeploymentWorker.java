package com.jarcadia.watchdog;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jarcadia.rcommando.RcCountDownLatch;
import com.jarcadia.rcommando.RcObject;
import com.jarcadia.rcommando.RcSet;
import com.jarcadia.rcommando.RcValues;
import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.retask.Retask;
import com.jarcadia.retask.Task;
import com.jarcadia.retask.TaskBucket;
import com.jarcadia.retask.annontations.RetaskChangeHandler;
import com.jarcadia.retask.annontations.RetaskHandler;
import com.jarcadia.retask.annontations.RetaskParam;
import com.jarcadia.retask.annontations.RetaskWorker;

@RetaskWorker
public class DeploymentWorker {

    private final Logger logger = LoggerFactory.getLogger(DeploymentWorker.class);

    private final RedisCommando rcommando;
//    private final RcSet deploymentsMap;
//    private final RcSet distributionsMap;

    protected DeploymentWorker(RedisCommando rcommando) {
        this.rcommando = rcommando;
    }

    @RetaskHandler("deploy")
    public void deploy(Retask retask, 
    		RcSet artifacts,
    		RcSet instances,
    		RcSet deployments,
    		RcSet distributions,
    		String artifactId,
    		Set<String> instanceIds,
    		TaskBucket bucket) {

        String cohortId = UUID.randomUUID().toString();
        Set<RcObject> deployInstances = instances.getSubset(instanceIds);

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
        if (!artifacts.has(artifactId)) {
            throw new DeploymentException("Artifact with id " + artifactId + " does not exist");
        }
        RcObject artifact = artifacts.get(artifactId);
        RcValues artifactValues = artifact.get("type", "version");
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
        Set<String> missingRoutes = retask.verifyRecruits(expectedRoutes);
        if (!missingRoutes.isEmpty()) {
            throw new DeploymentException("Missing deployment routes: " + missingRoutes.toString());
        }

        // Ensure none of the involved instances are already deploying
        Set<String> alreadyDeploying = rcommando.mergeIntoSetIfDistinct("deploy.active", instanceIds);
        if (alreadyDeploying.size() > 0) {
            throw new DeploymentException(String.format("%s is already part of active deployment", alreadyDeploying.toString()));
        }

        // Group the instances, according to their groupId or their ID if not part of a group
        Map<String, List<RcObject>> instancesByGroup = deployInstances.stream()
                .collect(Collectors.groupingBy(i -> i.get("group").isPresent() ? i.get("group").asString() : i.getId()));

        // Prepare the deployments
        for (List<RcObject> groupInstances : instancesByGroup.values()) {
            RcObject deployment = deployments.get(UUID.randomUUID().toString());
            deployment.checkedSet("type", type, "cohort", cohortId, "instances", groupInstances, "remaining", groupInstances);
            Task.create("deploy.advance").param("deployment", deployment).dropIn(bucket);

            for (RcObject instance : groupInstances) {
                instance.checkedSet("deployment", deployment, "deploymentState", DeployState.Waiting);
                setAdd("deploy.pending", instance);
            }
        }

        // Group the deployment instances by host 
        Map<String, List<RcObject>> instancesByHost = deployInstances.stream()
                .collect(Collectors.groupingBy(instance -> instance.get("host").asString()));

        // Create a distribution for each host
        for (String host : instancesByHost.keySet()) {
            List<RcObject> dependentInstances = instancesByHost.get(host);
            RcObject distribution = distributions.get(UUID.randomUUID().toString());
            distribution.checkedSet("state", DistributionState.PendingTransfer,
                    "type", type,
                    "host", host,
                    "artifact", artifact,
                    "version", artifactVersion,
                    "dependents", dependentInstances);

            Task.create("deploy.distribute")
                .param("distribution", distribution)
                .param("artifact", artifact)
                .param("host", host)
                .param("type", type)
                .dropIn(bucket);

            // Setup cleanup CDL for the distributions
            rcommando.getCdl(distribution.getId() + ".cleanup").init(dependentInstances.size());

            // Set the distribution for each instance
            for (RcObject instance : instancesByHost.get(host)) {
                instance.checkedSet("distribution", distribution);
            }
        }
    }

    @RetaskHandler("deploy.advance")
    public void advanceDeployment(RcObject deployment, TaskBucket bucket) {
        List<RcObject> remainingInstances = deployment.get("remaining").asListOf(RcObject.class);
        if (remainingInstances.isEmpty()) {
            completedDeploymentHandler(deployment);
        } else if (remainingInstances.size() == 1) {
            remainingInstances.get(0).checkedSet("deploymentState", DeployState.Ready);
        } else {
            String type = deployment.get("type").asString();
            Task.create("deploy.next." + type)
                    .param("deployment", deployment)
                    .param("remaining", remainingInstances)
                    .dropIn(bucket);
        }
    }
    
    private void completedDeploymentHandler(RcObject deployment) {
        // Update deploy state and remove instances from deploy.active
        List<RcObject> deploymentInstances = deployment.get("instances").asListOf(RcObject.class);
        for (RcObject instance : deploymentInstances) {
            instance.checkedClear("deploymentState");
            setRemove("deploy.active", instance);
        }
        deployment.checkedDelete();
        logger.info("Completed deployment {}", deployment.getId());
    }

    @RetaskChangeHandler(setKey = "instances", field = "deploymentState")
    public void instanceDeployStateChanged(@RetaskParam("object") RcObject instance, @RetaskParam("after") DeployState deployState, TaskBucket bucket) {
        if (deployState != null) {
            switch (deployState) {
                case Ready:
                    instanceIsReadyHandler(instance, bucket);
                    break;
                case Upgraded:
                    instanceIsUpgradedHandler(instance, bucket);
                    break;
            }
        }
    }

    private void instanceIsReadyHandler(RcObject instance, TaskBucket bucket) {
        if (setRemove("deploy.pending", instance)) {
            InstanceState state = instance.get("state").as(InstanceState.class);
            switch (state) {
                case Draining:
                case Enabled:
                    instance.checkedSet("deploymentState", DeployState.PendingDrain);
                    Task.create("deploy.disable").param("instance", instance).dropIn(bucket);
                    break;
                case Disabled:
                    instance.checkedSet("deploymentState", DeployState.PendingStop);
                    Task.create("deploy.stop").param("instance", instance).dropIn(bucket);
                    break;
                case Down:
                    Optional<RcObject> distribution = instance.get("distribution").asOptionalOf(RcObject.class);
                    if (distribution.isPresent()) {
                        instanceIsReadyForUpgradeHandler(instance, distribution.get(), bucket);
                    } else {
                        instance.checkedSet("deploymentState", DeployState.PendingStart);
                        Task.create("deploy.start").param("instance", instance).dropIn(bucket);
                    }
                    break;
                default:
                    logger.warn("Unexpected instance state {} for instance {}", state, instance.getId());
            }
        }
    }

    private void instanceIsUpgradedHandler(RcObject instance, TaskBucket bucket) {
        if (setRemove("deploy.pending-upgrade", instance)) {
            logger.info("Successfully upgraded {}", instance.getId());
            instance.checkedSet("deploymentState", DeployState.PendingStart);
            Task.create("deploy.start").param("instance", instance).dropIn(bucket);
            Optional<RcObject> distribution = instance.get("distribution").asOptionalOf(RcObject.class);
            if (distribution.isPresent()) {
                RcCountDownLatch cleanupCdl = rcommando.getCdl(distribution.get().getId() + ".cleanup");
                if (cleanupCdl.decrement()) {
                    logger.info("Cleaning up distribution {}", distribution.get().getId());
                    distribution.get().checkedSet("state", DistributionState.PendingCleanup);
                    Task.create("deploy.cleanup").param("distribution", distribution.get()).dropIn(bucket);
                }
            }
        }
    }

    @RetaskChangeHandler(setKey = "instances", field = "state")
    public void instanceStateChanged(@RetaskParam("object") RcObject instance, @RetaskParam("after") InstanceState state, TaskBucket bucket) {
        switch (state) {
            case Disabled:
                instanceIsDisabledHandler(instance, bucket);
                break;
            case Down:
                instanceIsDownHandler(instance, bucket);
                break;
            case Enabled:
                instanceIsEnabledHandler(instance, bucket);
                break;
        }
    }

    private void instanceIsDisabledHandler(RcObject instance, TaskBucket bucket) {
        if (setRemove("deploy.pending-disable", instance)) {
            instance.checkedSet("deploymentState", DeployState.PendingStop);
            Task.create("deploy.stop").param("instance", instance).dropIn(bucket);
        } else if (setRemove("deploy.pending-start", instance)) {
            instance.checkedSet("deploymentState", DeployState.PendingEnable);
            Task.create("deploy.enable").param("instance", instance).dropIn(bucket);
        }
    }

    private void instanceIsDownHandler(RcObject instance, TaskBucket bucket) {
        if (setRemove("deploy.pending-stop", instance)) {
            Optional<RcObject> distribution = instance.get("distribution").asOptionalOf(RcObject.class);
            if (distribution.isPresent()) {
                instanceIsReadyForUpgradeHandler(instance, distribution.get(), bucket);
            } else {
                instance.checkedSet("deploymentState", DeployState.PendingStart);
                Task.create("deploy.start").param("instance", instance).dropIn(bucket);
            }
        }
    }

    private void instanceIsEnabledHandler(RcObject instance, TaskBucket bucket) {
        if (setRemove("deploy.pending-enable", instance)) {
            RcObject deployment = instance.get("deployment").as(RcObject.class);
            logger.info("Completed instance {} in deployment {}", instance.getId(), deployment.getId());
            List<RcObject> remaining = deployment.get("remaining").asListOf(RcObject.class).stream()
                    .filter(i -> !instance.equals(i))
                    .collect(Collectors.toList());
            deployment.checkedSet("remaining", remaining);
            Task.create("deploy.advance").param("deployment", deployment).dropIn(bucket);
        }
    }

    @RetaskHandler("deploy.disable")
    public Task disable(RcObject instance) {
        String type = instance.get("type").asString();
        instance.checkedSet("deploymentState", DeployState.Draining);
        logger.info("Draining {} ({})", instance.getId(), type);
        rcommando.core().sadd("deploy.pending-disable", instance.getId());
        return Task.create("deploy.drain." + type).param("instance", instance);
    }

    @RetaskHandler("deploy.stop")
    public Task stop(RcObject instance) {
        String type = instance.get("type").asString();
        logger.info("Stopping {} ({})", instance.getId(), type);
        instance.checkedSet("deploymentState", DeployState.Stopping);
        rcommando.core().sadd("deploy.pending-stop", instance.getId());
        return Task.create("deploy.stop." + type).param("instance", instance);
    }

    @RetaskHandler("deploy.upgrade")
    public Task upgrade(RcObject instance) {
        RcValues values = instance.get("type", "host", "distribution");
        String type = values.next().asString();
        String host = values.next().asString();
        RcObject distribution = values.next().as(RcObject.class);
        RcObject artifact = distribution.get("artifact").as(RcObject.class);
        
        logger.info("Upgrading {} ({})", instance.getId(), type);

        rcommando.core().sadd("deploy.pending-upgrade", instance.getId());
        instance.checkedSet("deploymentState", DeployState.Upgrading);
        return Task.create("deploy.upgrade." + type)
                .param("host", host)
                .param("instance", instance)
                .param("distribution", distribution)
                .param("artifact", artifact);
    }

    @RetaskHandler("deploy.start")
    public Task start(RcObject instance) throws Exception {
        String type = instance.get("type").asString();
        logger.info("Starting {} ({})", instance.getId(), type);
        instance.checkedSet("deploymentState", DeployState.Starting);
        rcommando.core().sadd("deploy.pending-start", instance.getId());
        return Task.create("deploy.start." + type).param("instance", instance);
    }

    @RetaskHandler("deploy.enable")
    public Task enable(RcObject instance) throws Exception {
        String type = instance.get("type").asString();
        logger.info("Enabling {} ({})", instance.getId(), type);
        instance.checkedSet("deploymentState", DeployState.Enabling);
        rcommando.core().sadd("deploy.pending-enable", instance.getId());
        return Task.create("deploy.enable." + type).param("instance", instance);
    }

    @RetaskHandler("deploy.online")
    public Task online(RcObject instance) throws IOException {
        RcObject deployment = instance.get("deployment").as(RcObject.class);
        List<RcObject> remaining = deployment.get("remaining").asListOf(RcObject.class).stream()
                .filter(i -> !instance.equals(i))
                .collect(Collectors.toList());
        deployment.checkedSet("remaining", remaining);
        logger.info("Completed instance {} in deployment {}", instance.getId(), deployment.getId());
        return Task.create("deploy.advance").param("deployment", deployment);
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
    private void instanceIsReadyForUpgradeHandler(RcObject instance, RcObject distribution, TaskBucket bucket) {
        logger.info("Checking state of distribution {} before upgrading on {}", distribution.getId(), instance.getId());
        rcommando.core().sadd("deploy.pending-distribution", instance.getId());

        if (DistributionState.Transferred == distribution.get("state").as(DistributionState.class)) {
            // Distribution is complete
            if (rcommando.core().srem("deploy.pending-distribution", instance.getId()) == 1) {
                logger.info("Distribution was complete, upgrading instance");
                instance.checkedSet("deploymentState", DeployState.PendingUpgrade);
                Task.create("deploy.upgrade").param("instance", instance).dropIn(bucket);
            } else {
                logger.warn("Unlikely race condition occurred on instance {}, instance is expected to have been advanced by distribution {} state -> Verified",
                        instance.getId(), distribution.getId());
            }
        } else {
            // Distribution is not complete, set instance to PendingDistribution
            logger.info("Waiting for distribution {} to complete before advancing {}", distribution.getId(), instance.getId());
            instance.checkedSet("deploymentState", DeployState.PendingDistribution);
        }
    }

    @RetaskChangeHandler(setKey = "distributions", field = "state")
    public void distributionStateChange(@RetaskParam("object") RcObject distribution, DistributionState after, TaskBucket bucket) {
        logger.info("Distribution state change for {} -> {}", distribution.getId(), after);
        if (after == DistributionState.Transferred) {
            Set<RcObject> dependentInstances = distribution.get("dependents").asSetOf(RcObject.class);
            dependentInstances.stream()
                    .filter(depInst -> rcommando.core().srem("deploy.pending-distribution", depInst.getId()) == 1)
                    .map(depInst -> Task.create("deploy.upgrade").param("instance", depInst))
                    .forEach(task -> task.dropIn(bucket));
        } else if (after == DistributionState.CleanedUp) {
            distribution.checkedDelete();
        }
    }

    @RetaskHandler(value = "deploy.distribute")
    public Task distribute(String type, String host, RcObject distribution, RcObject artifact) {
        logger.info("Distributing artifact {} ({}) to {} for distribution {}", type, artifact.getId(), host, distribution.getId());
        distribution.checkedSet("state", DistributionState.Transferring);
        return Task.create("deploy.distribute." + type)
                .param("host", host)
                .param("distribution", distribution)
                .param("artifact", artifact);
    }

    @RetaskHandler("deploy.cleanup")
    public Task cleanup(RcObject distribution) {
        RcValues values = distribution.get("type", "host", "artifact");
        String type = values.next().asString();
        String host = values.next().asString();
        RcObject artifact = values.next().as(RcObject.class);
        logger.info("Cleaning up {} {} on {} for distribution {}", type, artifact.getId(), host, distribution.getId());
        distribution.checkedSet("state", DistributionState.PendingCleanup);
        return Task.create("deploy.cleanup." + type)
                .param("host", host)
                .param("distribution", distribution)
                .param("artifact", artifact);
    }

    private void setAdd(String set, RcObject instance) {
        rcommando.core().sadd(set, instance.getId());
    }

    private boolean setRemove(String set, RcObject instance) {
        return rcommando.core().srem(set, instance.getId()) == 1;
    }
}
