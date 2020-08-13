package com.jarcadia.watchdog;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jarcadia.rcommando.CountDownLatch;
import com.jarcadia.rcommando.ProxySet;
import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.rcommando.proxy.Proxy;
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
import com.jarcadia.watchdog.model.Group;
import com.jarcadia.watchdog.model.Instance;

@RetaskWorker
public class DeploymentWorker {

    private final Logger logger = LoggerFactory.getLogger(DeploymentWorker.class);

    private final RedisCommando rcommando;
    private final NotificationService notify;

    protected DeploymentWorker(RedisCommando rcommando, NotificationService notify) {
        this.rcommando = rcommando;
        this.notify = notify;
    }
    
    private interface Deployment extends Proxy {
    	
    	public String getApp();
    	public String getLabel();
    	public Optional<Artifact> getArtifact();
    	public Optional<DeployGroup> getGroup();
    	public List<DeployInstance> getInstances();
    	public List<DeployInstance> getRemaining();

    	public void setup(String app, String cohortId, DeployGroup group, String label, Artifact artifact, String version, List<DeployInstance> instances, List<DeployInstance> remaining, double progress);
    	public void setRemaining(List<DeployInstance> remaining);
    	public void setProgress(double progress);
    }

    private interface DeployInstance extends Instance {
    	
    	public Optional<DeployDistribution> getDistribution();
    	public Deployment getDeployment();
    	
    	public void setDeployment(Deployment deployment, DeployState deploymentState, double deploymentProgress);
    	public void setDeploymentState(DeployState deploymentState, double deploymentProgress);
    	public void setDistribution(DeployDistribution distribution);

    	// TODO implement multiclear
    	public void clearDeploymentState();
    	public void clearDeployment();
    	public void clearDistribution();
    }
    
    private interface DeployGroup extends Group {
    	
    	public void setDeployment(Deployment deployment);

    	public void clearDeployment();
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

    @RetaskHandler("deploy.artifact")
    public void deployUpgrade(RedisCommando rcommando, Retask retask, Artifact artifact, List<DeployInstance> instances, TaskBucket bucket) {
        try {
            deployHelper(rcommando, retask, artifact, instances, bucket);
        } catch (Exception ex) {
            notify.warn("Error while deploying: " + ex.getMessage());
            logger.warn("Error while deploying", ex);
        }
    }
    
    @RetaskHandler("deploy.restart")
    public void restart(RedisCommando rcommando, Retask retask, List<DeployInstance> instances, TaskBucket bucket) {
        try {
            deployHelper(rcommando, retask, null, instances, bucket);
        } catch (Exception ex) {
            notify.warn("Error while restarting: " + ex.getMessage());
            logger.warn("Error while restarting", ex);
        }
    }
    
    private void deployHelper(RedisCommando rcommando, Retask retask, Artifact artifact, List<DeployInstance> instances, TaskBucket bucket) {
    	boolean isUpgrade = artifact != null;
        String cohortId = UUID.randomUUID().toString();
        
        // Confirm all the instances exist
        List<String> nonexistent = instances.stream()
        		.filter(i -> !i.exists())
        		.map(i -> i.getSetKey() + ":" + i.getId())
        		.collect(Collectors.toList());
        if (nonexistent.size() > 0) {
        	throw new DeploymentException("Invalid deployment parameters. " + nonexistent.size() + " instances do not exist " + nonexistent.toString());
        }
        
        // Get all involved apps for use in validation
        List<String> apps = instances.stream()
                .map(i -> i.getApp())
                .distinct()
                .collect(Collectors.toList());
        
        if (isUpgrade) { // This block will validate arguments if this is an upgrade
        	
        	// Ensure all instances are the same app
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

            // Verify all expected routes are handled to upgrade this app
            List<String> expectedRoutes = Arrays.asList(
                    "deploy.next." + app, "deploy.drain." + app,
                    "deploy.stop." + app, "deploy.upgrade." + app,
                    "deploy.start." + app, "deploy.enable." + app,
                    "deploy.distribute." + app, "deploy.cleanup." + app);
            Set<String> missingRoutes = retask.verifyRecruits(expectedRoutes);
            if (!missingRoutes.isEmpty()) {
                throw new DeploymentException("Missing deployment routes: " + missingRoutes.toString());
            }
        
        } else {
        	// This block will validate arguments if this is a restart
        	
            // Verify all expected routes are handled to restart each app
            List<String> expectedRoutes = apps.stream()
            		.flatMap(app -> Stream.of("deploy.next." + app, "deploy.drain." + app, "deploy.stop." + app,
            				"deploy.start." + app, "deploy.enable." + app))
            		.collect(Collectors.toList());
            Set<String> missingRoutes = retask.verifyRecruits(expectedRoutes);
            if (!missingRoutes.isEmpty()) {
                throw new DeploymentException("Missing deployment routes: " + missingRoutes.toString());
            }
        }

        // Ensure none of the involved instances are already deploying
        Set<String> instanceIds = instances.stream().map(DeployInstance::getId).collect(Collectors.toSet());
        Set<String> alreadyDeploying = rcommando.mergeIntoSetIfDistinct("deploy.active", instanceIds); 
        if (alreadyDeploying.size() > 0) {
            throw new DeploymentException(String.format("%s is already part of active deployment", alreadyDeploying.toString()));
        }

        // Group the instances with other deploying instances if they share a Group, which ensures they are processed one at a time
        Map<String, List<DeployInstance>> instancesByGroup = instances.stream()
        		.filter(i -> i.getGroup().isPresent())
                .collect(Collectors.groupingBy(i -> i.getGroup().get().getId()));
        
        // Create deployments for all grouped instances
        int numDeployments = 0;
        ProxySet<Deployment> deploymentSet = rcommando.getSetOf("deployment", Deployment.class);
        ProxySet<DeployGroup> groupsSet = rcommando.getSetOf("group", DeployGroup.class);
        for (Entry<String, List<DeployInstance>> entry : instancesByGroup.entrySet()) {
        	String groupId = entry.getKey();
        	DeployGroup group = groupsSet.get(groupId);
        	String app = group.getApp();
        	List<DeployInstance> groupInstances = entry.getValue();
            Deployment deployment = deploymentSet.get(System.currentTimeMillis() + "_" + UUID.randomUUID().toString());
            
            // TODO set label to group / instance label instead of group id
            // Use instance label instead of group label if only a single instance is involved
            String label = groupInstances.size() == 1 ? groupInstances.get(0).getId() : group.getId();
            
            deployment.setup(app, cohortId, group, label, artifact, isUpgrade ? artifact.getVersion() : null, groupInstances, groupInstances, 0.0);
            Task.create("deploy.advance").param("deployment", deployment).dropIn(bucket);
            numDeployments++;

            // Set the group's deployment
            group.setDeployment(deployment);
            
            // Set the instance's deployment and state
            for (DeployInstance instance : groupInstances) {
                instance.setDeployment(deployment, DeployState.Waiting, 0.0);
                setAdd("deploy.pending", instance);
            }
        }
        
        // Create Deployments for the individual non-grouped instances
        List<DeployInstance> ungroupedInstances = instances.stream()
        		.filter(i -> i.getGroup().isEmpty())
        		.collect(Collectors.toList());
        for (DeployInstance instance : ungroupedInstances) {
            Deployment deployment = deploymentSet.get(System.currentTimeMillis() + "_" + UUID.randomUUID().toString());
            // TODO set label to instance label instead of instance ID
            deployment.setup(instance.getApp(), cohortId, null, instance.getId(), artifact, isUpgrade ? artifact.getVersion() : null, List.of(instance), List.of(instance), 0.0);
            Task.create("deploy.advance").param("deployment", deployment).dropIn(bucket);
            numDeployments++;
            instance.setDeployment(deployment, DeployState.Waiting, 0.0);
            setAdd("deploy.pending", instance);
        }
        
        // Create the distributions if this is an upgrade
        if (isUpgrade) {
        
            // Group the deployment instances by host 
            Map<String, List<DeployInstance>> instancesByHost = instances.stream()
                    .collect(Collectors.groupingBy(instance -> instance.getHost()));

            // Create a distribution for each host
            ProxySet<DeployDistribution> distributionSet = rcommando.getSetOf("distribution", DeployDistribution.class);
            for (String host : instancesByHost.keySet()) {
                List<DeployInstance> dependentInstances = instancesByHost.get(host);
                DeployDistribution distribution = distributionSet.get(UUID.randomUUID().toString());
                distribution.setup(artifact.getApp(), host, artifact, artifact.getVersion(), dependentInstances, DistributionState.PendingTransfer);

                Task.create("deploy.distribute")
                    .param("distribution", distribution)
                    .param("artifact", artifact)
                    .param("host", host)
                    .param("app", artifact.getApp())
                    .dropIn(bucket);

                // Setup cleanup CDL for the distributions
                rcommando.getCountDownLatch(distribution.getId() + ".cleanup").init(dependentInstances.size());

                // Set the distribution for each instance
                for (DeployInstance instance : instancesByHost.get(host)) {
                    instance.setDistribution(distribution);
                }
            }
        }
        notify.info("Started " + numDeployments + " deployments");
    }

    @RetaskHandler("deploy.advance")
    public void advanceDeployment(Retask retask, Deployment deployment) throws InterruptedException, ExecutionException {
        List<DeployInstance> remainingInstances = deployment.getRemaining();
        if (remainingInstances.isEmpty()) {
            completedDeploymentHandler(deployment);
        } else if (remainingInstances.size() == 1) {
            updateDeploymentState(deployment, remainingInstances.get(0), DeployState.Ready);
        } else {
            Task chooseNext = Task.create("deploy.next." + deployment.getApp())
                    .param("deployment", deployment)
                    .param("remaining", remainingInstances);
            
            Future<DeployInstance> future = retask.call(chooseNext, DeployInstance.class);
            DeployInstance next = future.get();
            updateDeploymentState(deployment, next, DeployState.Ready);
        }
    }
    
    private void completedDeploymentHandler(Deployment deployment) {
        // Update deploy state and remove instances from deploy.active
        List<DeployInstance> deploymentInstances = deployment.getInstances();
        for (DeployInstance instance : deploymentInstances) {
            instance.clearDeploymentState();
            setRemove("deploy.active", instance);
        }
        Optional<DeployGroup> group = deployment.getGroup();
        if (group.isPresent()) {
        	group.get().clearDeployment();
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
    	Deployment deployment = instance.getDeployment();
        if (setRemove("deploy.pending", instance)) {
            InstanceState state = instance.getState();
            switch (state) {
                case Draining:
                case Enabled:
                    updateDeploymentState(deployment, instance, DeployState.PendingDrain);
                    Task.create("deploy.disable").param("instance", instance).dropIn(bucket);
                    break;
                case Disabled:
                    updateDeploymentState(deployment, instance, DeployState.PendingStop);
                    Task.create("deploy.stop").param("instance", instance).dropIn(bucket);
                    break;
                case Down:
                    Optional<DeployDistribution> distribution = instance.getDistribution();
                    if (distribution.isPresent()) {
                        instanceIsReadyForUpgradeHandler(instance, distribution.get(), bucket);
                    } else {
                        updateDeploymentState(deployment, instance, DeployState.PendingStart);
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
            Deployment deployment = instance.getDeployment();
            updateDeploymentState(deployment, instance, DeployState.PendingStart);
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
        	updateDeploymentState(instance.getDeployment(), instance, DeployState.PendingStop);
            Task.create("deploy.stop").param("instance", instance).dropIn(bucket);
        } else if (setRemove("deploy.pending-start", instance)) {
        	updateDeploymentState(instance.getDeployment(), instance, DeployState.PendingEnable);
            Task.create("deploy.enable").param("instance", instance).dropIn(bucket);
        }
    }

    private void instanceIsDownHandler(DeployInstance instance, TaskBucket bucket) {
        if (setRemove("deploy.pending-stop", instance)) {
            Optional<DeployDistribution> distribution = instance.getDistribution();
            if (distribution.isPresent()) {
                instanceIsReadyForUpgradeHandler(instance, distribution.get(), bucket);
            } else {
                updateDeploymentState(instance.getDeployment(), instance, DeployState.PendingStart);
                Task.create("deploy.start").param("instance", instance).dropIn(bucket);
            }
        }
    }

    private void instanceIsEnabledHandler(DeployInstance instance, TaskBucket bucket) {
        if (setRemove("deploy.pending-enable", instance)) {
            Deployment deployment = instance.getDeployment();
            updateDeploymentState(instance.getDeployment(), instance, DeployState.Complete);
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
        updateDeploymentState(instance.getDeployment(), instance, DeployState.Draining);
        logger.info("Draining {} ({})", instance.getId(), instance.getApp());
        return Task.create("deploy.drain." + instance.getApp()).param("instance", instance);
    }

    @RetaskHandler("deploy.stop")
    public Task stop(DeployInstance instance) {
        setAdd("deploy.pending-stop", instance);
        updateDeploymentState(instance.getDeployment(), instance, DeployState.Stopping);
        logger.info("Stopping {} ({})", instance.getId(), instance.getApp());
        return Task.create("deploy.stop." + instance.getApp()).param("instance", instance);
    }

    @RetaskHandler("deploy.upgrade")
    public void upgrade(Retask retask, DeployInstance instance) throws InterruptedException, ExecutionException {
    	setAdd("deploy.pending-upgrade", instance);
    	Deployment deployment = instance.getDeployment();
        updateDeploymentState(deployment, instance, DeployState.Upgrading);
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
        updateDeploymentState(deployment, instance, DeployState.Upgraded);
    }

    @RetaskHandler("deploy.start")
    public Task start(DeployInstance instance) throws Exception {
    	setAdd("deploy.pending-start", instance);
        updateDeploymentState(instance.getDeployment(), instance, DeployState.Starting);
        logger.info("Starting {} ({})", instance.getId(), instance.getApp());
        return Task.create("deploy.start." + instance.getApp()).param("instance", instance);
    }

    @RetaskHandler("deploy.enable")
    public Task enable(DeployInstance instance) throws Exception {
    	setAdd("deploy.pending-enable", instance);
        updateDeploymentState(instance.getDeployment(), instance, DeployState.Enabling);
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
                updateDeploymentState(instance.getDeployment(), instance, DeployState.PendingUpgrade);
                Task.create("deploy.upgrade").param("instance", instance).dropIn(bucket);
            } else {
                logger.warn("Unlikely race condition occurred on instance {}, instance is expected to have been advanced by distribution {} state -> Verified",
                        instance.getId(), distribution.getId());
            }
        } else {
            // Distribution is not complete, set instance to PendingDistribution
            logger.info("Waiting for distribution {} to complete before advancing {}", distribution.getId(), instance.getId());
            updateDeploymentState(instance.getDeployment(), instance, DeployState.PendingDistribution);
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
    
    private void updateDeploymentState(Deployment deployment, DeployInstance instance, DeployState state) {
    	final boolean isUpgrade = deployment.getArtifact().isPresent();
    	final int instanceStep = isUpgrade ? getUpgradeStep(state) : getRestartStep(state);
    	final int instanceTotal = isUpgrade ? 14 : 10;
    	final int numRemaining = deployment.getRemaining().size();
    	final int numInstances = deployment.getInstances().size();
    	final int numComplete = Math.max(numInstances - numRemaining, 0);
    	final int deploymentStep = numComplete * instanceTotal + instanceStep;
    	final int deploymentTotal = numInstances * instanceTotal;
    	instance.setDeploymentState(state, percentize(instanceStep, instanceTotal));
    	deployment.setProgress(percentize(deploymentStep, deploymentTotal));
    	deployment.getInstances().size();
    }
    
    private double percentize(int step, int total) {
    	return Math.round(step * 100.0 / total) / 100.0;
    }
    
    private final int getUpgradeStep(DeployState state) {
    	final double total = 14;
    	switch (state) {
            case Waiting: return 0;
            case Ready: return 1;
            case PendingDrain: return 2;
            case Draining: return 3;
            case PendingStop: return 4;
            case Stopping: return 5;
            case PendingDistribution: return 6;
            case PendingUpgrade: return 7;
            case Upgrading: return 8;
            case Upgraded: return 9;
            case PendingStart: return 10;
            case Starting: return 11;
            case PendingEnable: return 12;
            case Enabling: return 13;
            case Complete: return 14;
    	}
    	throw new UnsupportedOperationException("Unrecognized DeployState " + state);
    }
    
    private final int getRestartStep(DeployState state) {
    	switch (state) {
            case Waiting: return 0;
            case Ready: return 1;
            case PendingDrain: return 2;
            case Draining: return 3;
            case PendingStop: return 4;
            case Stopping: return 5;
            case PendingStart: return 6;
            case Starting: return 7;
            case PendingEnable: return 8;
            case Enabling: return 9;
            case Complete: return 10;
    	}
    	throw new UnsupportedOperationException("Unrecognized DeployState " + state);
    }

    private void setAdd(String set, Proxy instance) {
        rcommando.core().sadd(set, instance.getId());
    }
    
    private boolean setRemove(String set, Proxy instance) {
        return rcommando.core().srem(set, instance.getId()) == 1;
    }
}
