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

import com.jarcadia.rcommando.CountDownLatch;
import com.jarcadia.rcommando.ProxySet;
import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.rcommando.proxy.DaoProxy;
import com.jarcadia.retask.Retask;
import com.jarcadia.retask.Task;
import com.jarcadia.retask.TaskBucket;
import com.jarcadia.retask.annontations.RetaskChangeHandler;
import com.jarcadia.retask.annontations.RetaskHandler;
import com.jarcadia.retask.annontations.RetaskParam;
import com.jarcadia.retask.annontations.RetaskWorker;
import com.jarcadia.watchdog.States.DistributionState;
import com.jarcadia.watchdog.States.InstanceState;
import com.jarcadia.watchdog.exception.DeploymentException;
import com.jarcadia.watchdog.model.Artifact;
import com.jarcadia.watchdog.model.Distribution;
import com.jarcadia.watchdog.model.Instance;

@RetaskWorker
public class DeploymentWorker {

    private final Logger logger = LoggerFactory.getLogger(DeploymentWorker.class);

    private final RedisCommando rcommando;

    protected DeploymentWorker(RedisCommando rcommando) {
        this.rcommando = rcommando;
    }
    
    private interface Deployment extends DaoProxy {
    	
    	public String getApp();
    	public List<DeployInstance> getInstances();
    	public List<DeployInstance> getRemaining();

    	public void setup(String app, String cohortId, List<DeployInstance> instances, List<DeployInstance> remaining);
    	public void setRemaining(List<DeployInstance> remaining);
    }

    private interface DeployInstance extends Instance {
    	
    	public Optional<DeployDistribution> getDistribution();
    	public Deployment getDeployment();
    	
    	public void setDeployment(Deployment deployment, DeployState deploymentState);
    	public void setDeploymentState(DeployState deploymentState);
    	public void setDistribution(DeployDistribution distribution);

    	// TODO implement multiclear
    	public void clearDeploymentState();
    	public void clearDeployment();
    	public void clearDistribution();
    }

    private interface DeployDistribution extends Distribution {
    	public List<DeployInstance> getDependents();
    	public void setup(String app, String host, Artifact artifact, String version, List<DeployInstance> dependents, DistributionState state);
    }

    @RetaskHandler("deploy.cancel.distribution")
    public void cancelDistribution(Retask retask, DeployDistribution distribution) throws InterruptedException, ExecutionException {
    	if (!distribution.exists()) {
    		throw new DeploymentException("Distribution " + distribution.getId() + " does not exist");
    	}
    	logger.info("Cancelling distribution {} for {}", distribution.getId(), distribution.getDependents());
    	for (DeployInstance instance : distribution.getDependents()) {
    		instance.clearDistribution();
    	}
    	
        Future<Void> future = retask.call(Task.create("deploy.cleanup." + distribution.getApp())
                .param("host", distribution.getHost())
                .param("distribution", distribution)
                .param("artifact", distribution.getArtifact()));
    	
        future.get();

    	distribution.delete();
    	logger.info("Distribution {} cancelled", distribution.getId());
    }

    @RetaskHandler("deploy.cancel.deployment")
    public void cancelDeployment(Deployment deployment) {
    	if (!deployment.exists()) {
    		throw new DeploymentException("Deployment " + deployment.getId() + " does not exist");
    	}
    	logger.info("Cancelling deployment {} for {}", deployment.getId(), deployment.getInstances());
    	for (DeployInstance instance : deployment.getInstances()) {
    		setRemove("deploy.active", instance);
    		setRemove("deploy.pending", instance);
    		setRemove("deploy.pending-disable", instance);
    		setRemove("deploy.pending-stop", instance);
    		setRemove("deploy.pending-upgrade", instance);
    		setRemove("deploy.pending-start", instance);
    		setRemove("deploy.pending-enable", instance);
    		setRemove("deploy.pending-distribution", instance);
    		instance.clearDeploymentState();
    		instance.clearDeployment();
    	}
    	deployment.delete();
    }

    @RetaskHandler("deploy")
    public void deploy(RedisCommando rcommando, Retask retask, Artifact artifact, List<DeployInstance> instances,
    		TaskBucket bucket) {

        String cohortId = UUID.randomUUID().toString();
        
        // Confirm all the instances exist
        List<String> nonexistent = instances.stream()
        		.filter(i -> !i.exists())
        		.map(i -> i.getSetKey() + ":" + i.getId())
        		.collect(Collectors.toList());
        if (nonexistent.size() > 0) {
        	throw new DeploymentException("Invalid deployment parameters. " + nonexistent.size() + " instances do not exist " + nonexistent.toString());
        }
        
        // Confirm all instances are the same app
        List<String> apps = instances.stream()
                .map(i -> i.getApp())
                .distinct()
                .collect(Collectors.toList());
        if (apps.size() > 1) {
            throw new DeploymentException("Instances with different apps cannot be part of the same deployment: " + apps.toString());
        }
        String app = apps.get(0);

        // Confirm the artifact exists and the app matches the instances' app
        if (!artifact.exists()) {
            throw new DeploymentException("Artifact with id " + artifact.getId() + " does not exist");
        }
        if (!app.equals(artifact.getApp())) {
            throw new DeploymentException("Artifact app " + artifact.getApp() + " for artifact " + artifact.getId() + " must match instance app " + app);
        }

        // Verify all expected routes are handled
        List<String> expectedRoutes = Arrays.asList(
                "deploy.next." + app, "deploy.drain." + app,
                "deploy.stop." + app, "deploy.upgrade." + app,
                "deploy.start." + app, "deploy.enable." + app,
                "deploy.distribute." + app, "deploy.cleanup." + app);
        Set<String> missingRoutes = retask.verifyRecruits(expectedRoutes);
        if (!missingRoutes.isEmpty()) {
            throw new DeploymentException("Missing deployment routes: " + missingRoutes.toString());
        }

        // Ensure none of the involved instances are already deploying
        Set<String> instanceIds = instances.stream().map(DeployInstance::getId).collect(Collectors.toSet());
        Set<String> alreadyDeploying = rcommando.mergeIntoSetIfDistinct("deploy.active", instanceIds); 
        if (alreadyDeploying.size() > 0) {
            throw new DeploymentException(String.format("%s is already part of active deployment", alreadyDeploying.toString()));
        }

        // Group the instances, according to their groupId or their ID if not part of a group
        Map<String, List<DeployInstance>> instancesByGroup = instances.stream()
                .collect(Collectors.groupingBy(i -> i.getGroup().isPresent() ? i.getGroup().get().getId() : i.getId()));

        // Prepare the deployments
        ProxySet<Deployment> deploymentSet = rcommando.getSetOf("deployment", Deployment.class);
        for (List<DeployInstance> groupInstances : instancesByGroup.values()) {
            Deployment deployment = deploymentSet.get(UUID.randomUUID().toString());
            deployment.setup(app, cohortId, groupInstances, groupInstances);
            Task.create("deploy.advance").param("deployment", deployment).dropIn(bucket);

            for (DeployInstance instance : groupInstances) {
                instance.setDeployment(deployment, DeployState.Waiting);
                setAdd("deploy.pending", instance);
            }
        }

        // Group the deployment instances by host 
        Map<String, List<DeployInstance>> instancesByHost = instances.stream()
                .collect(Collectors.groupingBy(instance -> instance.getHost()));

        // Create a distribution for each host
        ProxySet<DeployDistribution> distributionSet = rcommando.getSetOf("distribution", DeployDistribution.class);
        for (String host : instancesByHost.keySet()) {
            List<DeployInstance> dependentInstances = instancesByHost.get(host);
            DeployDistribution distribution = distributionSet.get(UUID.randomUUID().toString());
            distribution.setup(app, host, artifact, artifact.getVersion(), dependentInstances, DistributionState.PendingTransfer);

            Task.create("deploy.distribute")
                .param("distribution", distribution)
                .param("artifact", artifact)
                .param("host", host)
                .param("app", app)
                .dropIn(bucket);

            // Setup cleanup CDL for the distributions
            rcommando.getCountDownLatch(distribution.getId() + ".cleanup").init(dependentInstances.size());

            // Set the distribution for each instance
            for (DeployInstance instance : instancesByHost.get(host)) {
                instance.setDistribution(distribution);
            }
        }
    }

    @RetaskHandler("deploy.advance")
    public void advanceDeployment(Retask retask, Deployment deployment) throws InterruptedException, ExecutionException {
        List<DeployInstance> remainingInstances = deployment.getRemaining();
        if (remainingInstances.isEmpty()) {
            completedDeploymentHandler(deployment);
        } else if (remainingInstances.size() == 1) {
            remainingInstances.get(0).setDeploymentState(DeployState.Ready);
        } else {
            Task chooseNext = Task.create("deploy.next." + deployment.getApp())
                    .param("deployment", deployment)
                    .param("remaining", remainingInstances);
            
            Future<DeployInstance> future = retask.call(chooseNext, DeployInstance.class);
            DeployInstance next = future.get();
            next.setDeploymentState(DeployState.Ready);
        }
    }
    
    private void completedDeploymentHandler(Deployment deployment) {
        // Update deploy state and remove instances from deploy.active
        List<DeployInstance> deploymentInstances = deployment.getInstances();
        for (DeployInstance instance : deploymentInstances) {
            instance.clearDeploymentState();
            setRemove("deploy.active", instance);
        }
        deployment.delete();
        logger.info("Completed deployment {}", deployment.getId());
    }

    @RetaskChangeHandler(setKey = "instance", field = "deploymentState")
    public void instanceDeployStateChanged(@RetaskParam("object") DeployInstance instance, @RetaskParam("after") DeployState deployState, TaskBucket bucket) {
    	logger.info("Deployment state change {} -> {}", instance.getId(), deployState);
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

    private void instanceIsReadyHandler(DeployInstance instance, TaskBucket bucket) {
    	logger.info("Instance {} is ready", instance.getId());
        if (setRemove("deploy.pending", instance)) {
            InstanceState state = instance.getState();
            switch (state) {
                case Draining:
                case Enabled:
                	instance.setDeploymentState(DeployState.PendingDrain);
                    Task.create("deploy.disable").param("instance", instance).dropIn(bucket);
                    break;
                case Disabled:
                	instance.setDeploymentState(DeployState.PendingStop);
                    Task.create("deploy.stop").param("instance", instance).dropIn(bucket);
                    break;
                case Down:
                    Optional<DeployDistribution> distribution = instance.getDistribution();
                    if (distribution.isPresent()) {
                        instanceIsReadyForUpgradeHandler(instance, distribution.get(), bucket);
                    } else {
                        instance.setDeploymentState(DeployState.PendingStart);
                        Task.create("deploy.start").param("instance", instance).dropIn(bucket);
                    }
                    break;
                default:
                    logger.warn("Unexpected instance state {} for instance {}", state, instance.getId());
            }
        }
    }

    private void instanceIsUpgradedHandler(DeployInstance instance, TaskBucket bucket) {
        if (setRemove("deploy.pending-upgrade", instance)) {
            logger.info("Successfully upgraded {}", instance.getId());
            instance.setDeploymentState(DeployState.PendingStart);
            Task.create("deploy.start").param("instance", instance).dropIn(bucket);
            Optional<DeployDistribution> distribution = instance.getDistribution();
            if (distribution.isPresent()) {
                CountDownLatch cleanupCdl = rcommando.getCountDownLatch(distribution.get().getId() + ".cleanup");
                if (cleanupCdl.decrement()) {
                    logger.info("Cleaning up distribution {}", distribution.get().getId());
                    distribution.get().setState(DistributionState.PendingCleanup);
                    Task.create("deploy.cleanup").param("distribution", distribution.get()).dropIn(bucket);
                }
            }
        }
    }

    @RetaskChangeHandler(setKey = "instance", field = "state")
    public void instanceStateChanged(@RetaskParam("object") DeployInstance instance, @RetaskParam("after") InstanceState state, TaskBucket bucket) {
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

    private void instanceIsDisabledHandler(DeployInstance instance, TaskBucket bucket) {
        if (setRemove("deploy.pending-disable", instance)) {
            instance.setDeploymentState(DeployState.PendingStop);
            Task.create("deploy.stop").param("instance", instance).dropIn(bucket);
        } else if (setRemove("deploy.pending-start", instance)) {
            instance.setDeploymentState(DeployState.PendingEnable);
            Task.create("deploy.enable").param("instance", instance).dropIn(bucket);
        }
    }

    private void instanceIsDownHandler(DeployInstance instance, TaskBucket bucket) {
        if (setRemove("deploy.pending-stop", instance)) {
            Optional<DeployDistribution> distribution = instance.getDistribution();
            if (distribution.isPresent()) {
                instanceIsReadyForUpgradeHandler(instance, distribution.get(), bucket);
            } else {
                instance.setDeploymentState(DeployState.PendingStart);
                Task.create("deploy.start").param("instance", instance).dropIn(bucket);
            }
        }
    }

    private void instanceIsEnabledHandler(DeployInstance instance, TaskBucket bucket) {
        if (setRemove("deploy.pending-enable", instance)) {
            instance.setDeploymentState(DeployState.Complete);
            Deployment deployment = instance.getDeployment();
            logger.info("Completed instance {} in deployment {}", instance.getId(), deployment.getId());
            List<DeployInstance> remaining = deployment.getRemaining().stream()
                    .filter(i -> !instance.equals(i))
                    .collect(Collectors.toList());
            deployment.setRemaining(remaining);
            Task.create("deploy.advance").param("deployment", deployment).dropIn(bucket);
        }
    }

    @RetaskHandler("deploy.disable")
    public Task disable(DeployInstance instance) {
        setAdd("deploy.pending-disable", instance);
        instance.setDeploymentState(DeployState.Draining);
        logger.info("Draining {} ({})", instance.getId(), instance.getApp());
        return Task.create("deploy.drain." + instance.getApp()).param("instance", instance);
    }

    @RetaskHandler("deploy.stop")
    public Task stop(DeployInstance instance) {
        setAdd("deploy.pending-stop", instance);
        instance.setDeploymentState(DeployState.Stopping);
        logger.info("Stopping {} ({})", instance.getId(), instance.getApp());
        return Task.create("deploy.stop." + instance.getApp()).param("instance", instance);
    }

    @RetaskHandler("deploy.upgrade")
    public void upgrade(Retask retask, DeployInstance instance) throws InterruptedException, ExecutionException {
    	setAdd("deploy.pending-upgrade", instance);
        instance.setDeploymentState(DeployState.Upgrading);
        Distribution distribution = instance.getDistribution().get();
        Artifact artifact = distribution.getArtifact();
        logger.info("Upgrading {} ({})", instance.getId(), instance.getApp());

        Task upgradeTask = Task.create("deploy.upgrade." + instance.getApp())
                .param("host", instance.getHost())
                .param("instance", instance)
                .param("distribution", distribution)
                .param("artifact", artifact);

        Future<Void> future = retask.call(upgradeTask);
        future.get();
        instance.setDeploymentState(DeployState.Upgraded);
    }

    @RetaskHandler("deploy.start")
    public Task start(DeployInstance instance) throws Exception {
    	setAdd("deploy.pending-start", instance);
        instance.setDeploymentState(DeployState.Starting);
        logger.info("Starting {} ({})", instance.getId(), instance.getApp());
        return Task.create("deploy.start." + instance.getApp()).param("instance", instance);
    }

    @RetaskHandler("deploy.enable")
    public Task enable(DeployInstance instance) throws Exception {
    	setAdd("deploy.pending-enable", instance);
        instance.setDeploymentState(DeployState.Enabling);
        logger.info("Enabling {} ({})", instance.getId(), instance.getApp());
        return Task.create("deploy.enable." + instance.getApp()).param("instance", instance);
    }

//    @RetaskHandler("deploy.online")
//    public Task online(DeployInstance instance) throws IOException {
//        Deployment deployment = instance.getDeployment();
//        List<DeployInstance> remaining = deployment.getRemaining().stream()
//                .filter(i -> !instance.equals(i))
//                .collect(Collectors.toList());
//        deployment.setRemaining(remaining);
//        logger.info("Completed instance {} in deployment {}", instance.getId(), deployment.getId());
//        return Task.create("deploy.advance").param("deployment", deployment);
//    }

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
    private void instanceIsReadyForUpgradeHandler(DeployInstance instance, Distribution distribution, TaskBucket bucket) {
        logger.info("Checking state of distribution {} before upgrading on {}", distribution.getId(), instance.getId());
        setAdd("deploy.pending-distribution", instance);

        if (DistributionState.Transferred == distribution.getState()) {
            // Distribution is complete
            if (setRemove("deploy.pending-distribution", instance)) {
                logger.info("Distribution was complete, upgrading instance");
                instance.setDeploymentState(DeployState.PendingUpgrade);
                Task.create("deploy.upgrade").param("instance", instance).dropIn(bucket);
            } else {
                logger.warn("Unlikely race condition occurred on instance {}, instance is expected to have been advanced by distribution {} state -> Verified",
                        instance.getId(), distribution.getId());
            }
        } else {
            // Distribution is not complete, set instance to PendingDistribution
            logger.info("Waiting for distribution {} to complete before advancing {}", distribution.getId(), instance.getId());
            instance.setDeploymentState(DeployState.PendingDistribution);
        }
    }

    @RetaskChangeHandler(setKey = "distribution", field = "state")
    public void distributionStateChange(@RetaskParam("object") DeployDistribution distribution, DistributionState after, TaskBucket bucket) {
        logger.info("Distribution state change for {} -> {}", distribution.getId(), after);
        if (after == DistributionState.Transferred) {
            distribution.getDependents().stream()
                    .filter(depInst -> setRemove("deploy.pending-distribution", depInst))
                    .map(depInst -> Task.create("deploy.upgrade").param("instance", depInst))
                    .forEach(task -> task.dropIn(bucket));
        } else if (after == DistributionState.CleanedUp) {
            distribution.delete();
        }
    }

    @RetaskHandler(value = "deploy.distribute")
    public Task distribute(String app, String host, DeployDistribution distribution, Artifact artifact) {
        logger.info("Distributing artifact {} ({}) to {} for distribution {}", app, artifact.getId(), host, distribution.getId());
        distribution.setState(DistributionState.Transferring);
        return Task.create("deploy.distribute." + app)
                .param("host", host)
                .param("distribution", distribution)
                .param("artifact", artifact);
    }

    @RetaskHandler("deploy.cleanup")
    public Task cleanup(DeployDistribution distribution) {
        logger.info("Cleaning up {} {} on {} for distribution {}", distribution.getApp(),
        		distribution.getArtifact().getId(), distribution.getHost(), distribution.getId());
        distribution.setState(DistributionState.PendingCleanup);
        return Task.create("deploy.cleanup." + distribution.getApp())
                .param("host", distribution.getHost())
                .param("distribution", distribution)
                .param("artifact", distribution.getArtifact());
    }

    private void setAdd(String set, DaoProxy instance) {
        rcommando.core().sadd(set, instance.getId());
    }
    
    private boolean setRemove(String set, DaoProxy instance) {
        return rcommando.core().srem(set, instance.getId()) == 1;
    }
}
