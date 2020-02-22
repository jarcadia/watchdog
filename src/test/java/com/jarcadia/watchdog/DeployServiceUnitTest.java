package com.jarcadia.watchdog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jarcadia.rcommando.Dao;
import com.jarcadia.rcommando.DaoSet;
import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.retask.Retask;
import com.jarcadia.retask.RetaskManager;
import com.jarcadia.retask.RetaskRecruiter;
import com.jarcadia.retask.Task;
import com.jarcadia.retask.annontations.RetaskChangeHandler;
import com.jarcadia.retask.annontations.RetaskDeleteHandler;
import com.jarcadia.retask.annontations.RetaskHandler;
import com.jarcadia.retask.annontations.RetaskInsertHandler;
import com.jarcadia.retask.annontations.RetaskParam;
import com.jarcadia.retask.annontations.RetaskWorker;
import com.jarcadia.watchdog.States.DistributionState;
import com.jarcadia.watchdog.States.InstanceState;
import com.jarcadia.watchdog.model.Artifact;
import com.jarcadia.watchdog.model.Group;

import io.lettuce.core.RedisClient;
import io.netty.util.internal.ThreadLocalRandom;

@RetaskWorker
public class DeployServiceUnitTest {

    private final Logger logger = LoggerFactory.getLogger(DeployServiceUnitTest.class);
    
    
    private RetaskRecruiter recruiter() {
    	RetaskRecruiter recruiter = new RetaskRecruiter();
        recruiter.recruitFromClass(DeploymentWorker.class);
        recruiter.recruitFromClass(DeployServiceUnitTest.class);
//        recruiter.recruitFromPackage("com.jarcadia.watchdog.model");
        recruiter.recruitFromClass(Group.class); // Group is referenced only indirectly, must be included explicitly
        recruiter.recruitFromClass(Artifact.class); // Artifact is referenced only indirectly, must be included explicitly
        return recruiter;
    }
    
    @Test
    public void testSingleInsanceDeployment() throws Exception {
    	RedisClient redisClient = RedisClient.create("redis://localhost:6379/13");
    	
        RedisCommando rcommando = RedisCommando.create(redisClient);
        rcommando.core().flushdb();
        RetaskManager manager = Retask.init(redisClient, rcommando, recruiter());

        // Setup instances
        DaoSet instances = rcommando.getSetOf("instance");
        instances.get("inst").set("app", "webserver", "group", "group:group", "host", "web01", "port", 8080, "state", InstanceState.Enabled);
        
        // Setup artifact
        Dao artifact = rcommando.getSetOf("artifact").get("webserver_1.0");
        artifact.set("app", "webserver", "version", "1.0");
        
        // Setup test Deployment implementation
        TestDeploymentImpl testDeployImpl = Mockito.spy(new TestDeploymentImpl());
        manager.addInstance(TestDeploymentImpl.class, testDeployImpl);

        // Setup DeploymentStateRecorder for assertions
        StateRecorder deployStateRecorder = new StateRecorder(rcommando);
        manager.addInstance(StateRecorder.class, deployStateRecorder);

        // Create DeployWorker under test
        DeploymentWorker deploymentWorker = new DeploymentWorker(rcommando);
        manager.addInstance(DeploymentWorker.class, deploymentWorker);

        // Start retask and submit deploy task 
        manager.start(Task.create("deploy")
                    .param("instances", List.of("instance:inst"))
                    .param("artifact", "artifact:webserver_1.0"));

        // Wait for the deployment to complete
        deployStateRecorder.awaitCompletion(1, TimeUnit.SECONDS);

        // Verify the instances deploy states progressed as expected
        Assertions.assertIterableEquals(expectedDeployStates(), deployStateRecorder.getStates("inst"));

        // Verify the deployment agent spy callbacks were invoked in order
        verifyDeployAgent(testDeployImpl, rcommando.getSetOf("instance").get("inst"), artifact);

        // Shutdown retask
        manager.shutdown(1, TimeUnit.SECONDS);
    }

    @Test
    public void testTwoInstanceDeployment() throws Exception {
    	RedisClient redisClient = RedisClient.create("redis://localhost/14");
        RedisCommando rcommando = RedisCommando.create(redisClient);
        rcommando.core().flushdb();
        RetaskManager manager = Retask.init(redisClient, rcommando, recruiter());

        // Setup instances
        DaoSet instances = rcommando.getSetOf("instance");
        instances.get("inst1").set("app", "webserver", "group", "group:group", "host", "web01", "port", 8080, "state", InstanceState.Enabled);
        instances.get("inst2").set("app", "webserver", "group", "group:group", "host", "web02", "port", 8080, "state", InstanceState.Enabled);
        
        // Setup groups
        DaoSet groups = rcommando.getSetOf("group");
        groups.get("group").set("instances", Arrays.asList("inst1", "inst2"));

        // Setup artifact
        Dao artifact = rcommando.getSetOf("artifact").get("webserver_1.0");
        artifact.set("app", "webserver", "version", "1.0");

        // Setup spied test Deployment implementation
        TestDeploymentImpl deploymentImpl = Mockito.spy(new TestDeploymentImpl());
        manager.addInstance(TestDeploymentImpl.class, deploymentImpl);

        // Setup test StateRecorder for assertions
        StateRecorder stateRecorder = new StateRecorder(rcommando);
        manager.addInstance(StateRecorder.class, stateRecorder);

        // Create DeployWorker under test
        DeploymentWorker deployWorker = new DeploymentWorker(rcommando);
        manager.addInstance(DeploymentWorker.class, deployWorker);

        // Submit task to start deployment
        manager.start(Task.create("deploy")
                .param("instances",  Arrays.asList("instance:inst1", "instance:inst2"))
                .param("artifact", "artifact:webserver_1.0"));

        // Wait for the deployment to complete
        stateRecorder.awaitCompletion(1, TimeUnit.SECONDS);

        // Verify the instances deploy states progressed as expected
        Assertions.assertIterableEquals(expectedDeployStates(), stateRecorder.getStates("inst1"));
        Assertions.assertIterableEquals(expectedDeployStates(), stateRecorder.getStates("inst2"));

        // Verify the deployment agent spy callbacks were invoked in order
        verifyDeployAgent(deploymentImpl, rcommando.getSetOf("instance").get("inst1"), artifact);
        verifyDeployAgent(deploymentImpl, rcommando.getSetOf("instance").get("inst2"), artifact);

        // Shutdown retask
        manager.shutdown(1, TimeUnit.SECONDS);
    }

    @Test
    public void testLargeMultiDeployment() throws Exception {
    	RedisClient redisClient = RedisClient.create("redis://localhost/15");
        RedisCommando rcommando = RedisCommando.create(redisClient);
        rcommando.core().flushdb();
        RetaskManager manager = Retask.init(redisClient, rcommando, recruiter());
        
        // Setup instances and groups
        int numGroups = 50;
        String[][] hosts = {{"web01", "web02"}, {"web03", "web04"}, {"web05", "web06"}, {"web07", "web08"}};
        DaoSet instanceSet = rcommando.getSetOf("instance");
        DaoSet groups = rcommando.getSetOf("group");
        List<Dao> instances = new ArrayList<>();
        for (int i=0; i<numGroups; i++) {
            String groupId = "group" + i;
            String inst1Id = groupId + "-" + "inst1";
            String inst2Id = groupId + "-" + "inst2";

            String[] groupHosts = hosts[ThreadLocalRandom.current().nextInt(hosts.length)];
            Dao inst1 = instanceSet.get(inst1Id);
            inst1.set("app", "webserver", "group", "group:" + groupId, "host", groupHosts[0], "port", 8080 + i, "state", InstanceState.Enabled);
            Dao inst2 = instanceSet.get(inst2Id);
            inst2.set("app", "webserver", "group", "group:" + groupId, "host", groupHosts[1], "port", 8080 + i, "state", InstanceState.Enabled);
            groups.get(groupId).set("instances", Arrays.asList(inst1, inst2));

            instances.add(inst1);
            instances.add(inst2);
        }

        // Setup mocked artifact
        Dao artifact = rcommando.getSetOf("artifact").get("webserver_1.0");
        artifact.set("app", "webserver", "version", "1.0");

        // Setup spied TestDeploymentWorker 
        TestDeploymentImpl testDeploymentWorker = Mockito.spy(new TestDeploymentImpl());

        // Setup DeploymentStateRecorder for assertions
        StateRecorder deployStateRecorder = new StateRecorder(rcommando);

        // Create Deploy Service for test
        DeploymentWorker deployService = new DeploymentWorker(rcommando);

        manager.addInstance(TestDeploymentImpl.class, testDeploymentWorker);
        manager.addInstance(StateRecorder.class, deployStateRecorder);
        manager.addInstance(DeploymentWorker.class, deployService);

        // Submit task to start deployment
        manager.start(Task.create("deploy")
                .param("instances",  instances)
                .param("artifact", "artifact:webserver_1.0"));

        // Wait for the deployment to complete
        deployStateRecorder.awaitCompletion(10, TimeUnit.SECONDS);

        for (Dao instance : instances) {
            Assertions.assertIterableEquals(expectedDeployStates(), deployStateRecorder.getStates(instance.getId()));
            verifyDeployAgent(testDeploymentWorker, instance, artifact);
        }
        manager.shutdown(1, TimeUnit.SECONDS);
    }

    private List<DeployState> expectedDeployStates() {
        return Arrays.asList(DeployState.Waiting, DeployState.Ready,
                DeployState.PendingDrain, DeployState.Draining, 
                DeployState.PendingStop, DeployState.Stopping,
                DeployState.PendingUpgrade, DeployState.Upgrading, DeployState.Upgraded,
                DeployState.PendingStart, DeployState.Starting,
                DeployState.PendingEnable, DeployState.Enabling, null);
    }

    private void verifyDeployAgent(TestDeploymentImpl agent, Dao expected, Dao artifact) throws Exception {
        InOrder depVerifer = Mockito.inOrder(agent);
        depVerifer.verify(agent, Mockito.times(1)).disable(expected);
        depVerifer.verify(agent, Mockito.times(1)).stop(expected);
        depVerifer.verify(agent, Mockito.times(1)).upgrade(Mockito.eq(expected), Mockito.eq(artifact), Mockito.any());
        depVerifer.verify(agent, Mockito.times(1)).start(expected);
        depVerifer.verify(agent, Mockito.times(1)).join(expected);
    }
    
    @RetaskWorker
    public class TestDeploymentImpl {
        
        @RetaskHandler("deploy.drain.webserver")
        public void disable(Dao instance) {
            logger.info("Draining {}", instance.getId());
            instance.set("state", InstanceState.Disabled);
        }

        @RetaskHandler("deploy.stop.webserver")
        public void stop(Dao instance) {
            logger.info("Stopping {}", instance.getId());
            instance.set("state", InstanceState.Down);
        }

        @RetaskHandler("deploy.upgrade.webserver")
        public void upgrade(Dao instance, Dao artifact, Dao distribution) {
            logger.info("Upgrading {}", instance.getId());
        }

        @RetaskHandler("deploy.start.webserver")
        public void start(Dao instance) {
            logger.info("Starting {}", instance.getId());
            instance.set("state", InstanceState.Disabled);
        }

        @RetaskHandler("deploy.enable.webserver")
        public void join(Dao instance) {
            logger.info("Enabling {}", instance.getId());
            instance.set("state", InstanceState.Enabled);
        }

        @RetaskHandler("deploy.distribute.webserver")
        public void distribute(String host, Dao distribution, Dao artifact) throws InterruptedException {
            distribution.set("state", DistributionState.Transferred);
        }

        @RetaskHandler("deploy.cleanup.webserver")
        public void cleanup(String host, Dao distribution, Dao artifact) {
            distribution.set("state", DistributionState.CleanedUp);
        }
        
        @RetaskHandler("deploy.next.webserver")
        public Dao chooseNext(Dao deployment, List<Dao> remaining) {
            return remaining.get(0);
        }
    }

    @RetaskWorker
    public class StateRecorder {
    	
    	private final RedisCommando rcommando;
    	private final String guid;
    	private final String activeKey;

        public StateRecorder(RedisCommando rcommando) {
        	this.rcommando = rcommando;
        	this.guid = UUID.randomUUID().toString();
        	this.activeKey = key("active");
        }
        
        @RetaskInsertHandler("deployment")
        public void deploymentInserted(@RetaskParam("object") Dao deployment) {
        	logger.info("Deployment {} was created", deployment.getId());
        	rcommando.core().incr(activeKey);
        }

        @RetaskDeleteHandler("deployment")
        public void deploymentDeleted(String id) {
        	logger.info("Deployment {} was deleted", id);
        	rcommando.core().decr(activeKey);
        }

        @RetaskChangeHandler(setKey = "instance", field = "deploymentState")
        public void changeState(@RetaskParam("object") Dao instance, DeployState before, DeployState after) {
            logger.info("State change for {}: {} -> {}", instance.getId(), before, after);
        	rcommando.core().rpush(key(instance.getId()), after == null ? "null" : after.toString());
        }

        public void awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            while (!isDone()) {
                Thread.sleep(100);
            }
        }
        
        private boolean isDone() {
            return "0".equals(rcommando.core().get(activeKey));
        }

        public List<DeployState> getStates(String instanceId) {
            return rcommando.core().lrange(key(instanceId), 0, -1).stream()
            		.map(str -> str.equals("null") ? null : DeployState.valueOf(str))
            		.collect(Collectors.toList());
        }

        private String key(String id) {
        	return "recorder." + guid + "." + id;
        }
    }
}
