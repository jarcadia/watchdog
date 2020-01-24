package com.jarcadia.watchdog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

import com.jarcadia.rcommando.RcObject;
import com.jarcadia.rcommando.RcSet;
import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.retask.Retask;
import com.jarcadia.retask.RetaskContext;
import com.jarcadia.retask.RetaskManager;
import com.jarcadia.retask.RetaskRecruiter;
import com.jarcadia.retask.Task;
import com.jarcadia.retask.annontations.RetaskChangeHandler;
import com.jarcadia.retask.annontations.RetaskDeleteHandler;
import com.jarcadia.retask.annontations.RetaskHandler;
import com.jarcadia.retask.annontations.RetaskInsertHandler;
import com.jarcadia.retask.annontations.RetaskParam;
import com.jarcadia.retask.annontations.RetaskWorker;

import io.lettuce.core.RedisClient;
import io.netty.util.internal.ThreadLocalRandom;

@RetaskWorker
public class DeployServiceUnitTest {

    private final Logger logger = LoggerFactory.getLogger(DeployServiceUnitTest.class);
    
    @Test
    public void testSingleInstanceDeployment() throws Exception {
    	RedisClient redisClient = RedisClient.create("redis://localhost/13");
    	RetaskRecruiter recruiter = new RetaskRecruiter();
        recruiter.recruitFromClass(DeploymentWorker.class);
        recruiter.recruitFromClass(StateRecorder.class);
        recruiter.recruitFromClass(TestDeploymentImpl.class);

        RedisCommando rcommando = RedisCommando.create(redisClient);
        rcommando.core().flushdb();
        LazyInstanceProvider provider = new LazyInstanceProvider();
        RetaskManager manager = Retask.init(redisClient, rcommando, recruiter, provider);

        // Setup instances
        RcSet instances = rcommando.getSetOf("instances");
        instances.get("inst").set("type", "webserver", "group", "group", "host", "web01", "port", 8080, "state", InstanceState.Enabled);
        
        // Setup artifact
        RcObject artifact = rcommando.getSetOf("artifacts").get("webserver_1.0");
        artifact.checkedSet("type", "webserver", "version", "1.0");
        
        // Setup test Deployment implementation
        TestDeploymentImpl testDeployImpl = Mockito.spy(new TestDeploymentImpl());
        provider.set(TestDeploymentImpl.class, testDeployImpl);

        // Setup DeploymentStateRecorder for assertions
        StateRecorder deployStateRecorder = new StateRecorder(rcommando);
        provider.set(StateRecorder.class, deployStateRecorder);

        // Create DeployWorker under test
        DeploymentWorker deploymentWorker = new DeploymentWorker(rcommando);
        provider.set(DeploymentWorker.class, deploymentWorker);

        // Start retask and submit deploy task 
        manager.start(Task.create("deploy")
                    .param("instanceIds", Arrays.asList("inst"))
                    .param("artifactId", "webserver_1.0"));

        // Wait for the deployment to complete
        deployStateRecorder.awaitCompletion(1, TimeUnit.SECONDS);

        // Verify the instances deploy states progressed as expected
        System.out.println( deployStateRecorder.getStates("inst"));
        Assertions.assertIterableEquals(expectedDeployStates(), deployStateRecorder.getStates("inst"));

        // Verify the deployment agent spy callbacks were invoked in order
        verifyDeployAgent(testDeployImpl, "instances", "inst", artifact);

        // Shutdown retask
        manager.shutdown(1, TimeUnit.SECONDS);
    }

    @Test
    public void testTwoInstanceDeployment() throws Exception {
    	RedisClient redisClient = RedisClient.create("redis://localhost/14");
    	RetaskRecruiter recruiter = new RetaskRecruiter();
        recruiter.recruitFromClass(DeploymentWorker.class);
        recruiter.recruitFromClass(StateRecorder.class);
        recruiter.recruitFromClass(TestDeploymentImpl.class);
        
        RedisCommando rcommando = RedisCommando.create(redisClient);
        rcommando.core().flushdb();
        LazyInstanceProvider provider = new LazyInstanceProvider();
        RetaskManager manager = Retask.init(redisClient, rcommando, recruiter, provider);

        // Setup instances
        RcSet instances = rcommando.getSetOf("instances");
        instances.get("inst1").set("type", "webserver", "group", "group", "host", "web01", "port", 8080, "state", InstanceState.Enabled);
        instances.get("inst2").set("type", "webserver", "group", "group", "host", "web02", "port", 8080, "state", InstanceState.Enabled);
        
        // Setup groups
        RcSet groups = rcommando.getSetOf("groups");
        groups.get("group").set("instances", Arrays.asList("inst1", "inst2"));

        // Setup artifact
        RcObject artifact = rcommando.getSetOf("artifacts").get("webserver_1.0");
        artifact.checkedSet("type", "webserver", "version", "1.0");

        // Setup spied test Deployment implementation
        TestDeploymentImpl deploymentImpl = Mockito.spy(new TestDeploymentImpl());
        provider.set(TestDeploymentImpl.class, deploymentImpl);

        // Setup test StateRecorder for assertions
        StateRecorder stateRecorder = new StateRecorder(rcommando);
        provider.set(StateRecorder.class, stateRecorder);

        // Create DeployWorker under test
        DeploymentWorker deployWorker = new DeploymentWorker(rcommando);
        provider.set(DeploymentWorker.class, deployWorker);

        // Submit task to start deployment
        manager.start(Task.create("deploy")
                .param("instanceIds",  Arrays.asList("inst1", "inst2"))
                .param("artifactId", "webserver_1.0"));

        // Wait for the deployment to complete
        stateRecorder.awaitCompletion(1, TimeUnit.SECONDS);

        // Verify the instances deploy states progressed as expected
        System.out.println(stateRecorder.getStates("inst1"));
        Assertions.assertIterableEquals(expectedDeployStates(), stateRecorder.getStates("inst1"));
        Assertions.assertIterableEquals(expectedDeployStates(), stateRecorder.getStates("inst2"));

        // Verify the deployment agent spy callbacks were invoked in order
        verifyDeployAgent(deploymentImpl, "instances", "inst1", artifact);
        verifyDeployAgent(deploymentImpl, "instances", "inst2", artifact);

        // Shutdown retask
        manager.shutdown(1, TimeUnit.SECONDS);
    }

    @Test
    public void testLargeMultiDeployment() throws Exception {
    	RedisClient redisClient = RedisClient.create("redis://localhost/15");
    	RetaskRecruiter recruiter = new RetaskRecruiter();
        recruiter.recruitFromClass(DeploymentWorker.class);
        recruiter.recruitFromClass(StateRecorder.class);
        recruiter.recruitFromClass(TestDeploymentImpl.class);
        
        
        RedisCommando rcommando = RedisCommando.create(redisClient);
        rcommando.core().flushdb();
        LazyInstanceProvider provider = new LazyInstanceProvider();
        RetaskManager manager = Retask.init(redisClient, rcommando, recruiter, provider);
        
        // Setup instances and groups
        int numGroups = 50;
        String[][] hosts = {{"web01", "web02"}, {"web03", "web04"}, {"web05", "web06"}, {"web07", "web08"}};
        RcSet instances = rcommando.getSetOf("instances");
        RcSet groups = rcommando.getSetOf("groups");
        List<String> instanceIds = new ArrayList<>();
        for (int i=0; i<numGroups; i++) {
            String groupId = "group" + i;
            String inst1Id = groupId + "-" + "inst1";
            String inst2Id = groupId + "-" + "inst2";

            instanceIds.add(inst1Id);
            instanceIds.add(inst2Id);
            String[] groupHosts = hosts[ThreadLocalRandom.current().nextInt(hosts.length)];
            instances.get(inst1Id).set("type", "webserver", "group", groupId, "host", groupHosts[0], "port", 8080 + i, "state", InstanceState.Enabled);
            instances.get(inst2Id).set("type", "webserver", "group", groupId, "host", groupHosts[1], "port", 8080 + i, "state", InstanceState.Enabled);
            groups.get(groupId).set("instances", Arrays.asList(inst1Id, inst2Id));
        }

        // Setup mocked artifact
        RcObject artifact = rcommando.getSetOf("artifacts").get("webserver_1.0");
        artifact.checkedSet("type", "webserver", "version", "1.0");

        // Setup spied TestDeploymentWorker 
        TestDeploymentImpl testDeploymentWorker = Mockito.spy(new TestDeploymentImpl());
        provider.set(TestDeploymentImpl.class, testDeploymentWorker);

        // Setup DeploymentStateRecorder for assertions
        StateRecorder deployStateRecorder = new StateRecorder(rcommando);
        provider.set(StateRecorder.class, deployStateRecorder);

        // Create Deploy Service for test
        DeploymentWorker deployService = new DeploymentWorker(rcommando);
        provider.set(DeploymentWorker.class, deployService);

        // Submit task to start deployment
        manager.start(Task.create("deploy")
                .param("instanceIds",  instanceIds)
                .param("artifactId", "webserver_1.0"));

        // Wait for the deployment to complete
        deployStateRecorder.awaitCompletion(10, TimeUnit.SECONDS);

        for (String instanceId : instanceIds) {
            Assertions.assertIterableEquals(expectedDeployStates(), deployStateRecorder.getStates(instanceId));
            verifyDeployAgent(testDeploymentWorker, "instances", instanceId, artifact);
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

    private void verifyDeployAgent(TestDeploymentImpl agent, String mapKey, String id, RcObject artifact) throws Exception {
        InOrder depVerifer = Mockito.inOrder(agent);
        RcObject expected = new RcObject(null, null, mapKey, id);
        depVerifer.verify(agent, Mockito.times(1)).disable(expected);
        depVerifer.verify(agent, Mockito.times(1)).stop(expected);
        depVerifer.verify(agent, Mockito.times(1)).upgrade(Mockito.eq(expected), Mockito.eq(artifact), Mockito.any());
        depVerifer.verify(agent, Mockito.times(1)).start(expected);
        depVerifer.verify(agent, Mockito.times(1)).join(expected);
    }
    
    @RetaskWorker
    public class TestDeploymentImpl {
        
        @RetaskHandler("deploy.drain.webserver")
        public void disable(RcObject instance) {
            logger.info("Draining {}", instance.getId());
            instance.checkedSet("state", InstanceState.Disabled);
        }

        @RetaskHandler("deploy.stop.webserver")
        public void stop(RcObject instance) {
            logger.info("Stopping {}", instance.getId());
            instance.checkedSet("state", InstanceState.Down);
        }

        @RetaskHandler("deploy.upgrade.webserver")
        public void upgrade(RcObject instance, RcObject artifact, RcObject distribution) {
            logger.info("Upgrading {}", instance.getId());
            instance.checkedSet("deploymentState", DeployState.Upgraded);
        }

        @RetaskHandler("deploy.start.webserver")
        public void start(RcObject instance) {
            logger.info("Starting {}", instance.getId());
            instance.checkedSet("state", InstanceState.Disabled);
        }

        @RetaskHandler("deploy.enable.webserver")
        public void join(RcObject instance) {
            logger.info("Enabling {}", instance.getId());
            instance.checkedSet("state", InstanceState.Enabled);
        }

        @RetaskHandler("deploy.distribute.webserver")
        public void distribute(String host, RcObject distribution, RcObject artifact) throws InterruptedException {
            distribution.checkedSet("state", DistributionState.Transferred);
        }

        @RetaskHandler("deploy.cleanup.webserver")
        public void cleanup(String host, RcObject distribution, RcObject artifact) {
            distribution.checkedSet("state", DistributionState.CleanedUp);
        }
        
        @RetaskHandler("deploy.next.webserver")
        public void chooseNext(RcObject deployment, List<RcObject> remaining) {
            RcObject next = remaining.get(0);
            next.checkedSet("deploymentState", DeployState.Ready);
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
        
        @RetaskInsertHandler("deployments")
        public void deploymentInserted(@RetaskParam("object") RcObject deployment) {
        	logger.info("Deployment {} was created", deployment.getId());
        	rcommando.core().incr(activeKey);
        }

        @RetaskDeleteHandler("deployments")
        public void deploymentDeleted(String id) {
        	logger.info("Deployment {} was deleted", id);
        	rcommando.core().decr(activeKey);
        }

        @RetaskChangeHandler(setKey = "instances", field = "deploymentState")
        public void changeState(@RetaskParam("object") RcObject instance, DeployState before, DeployState after) {
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

    static class LazyInstanceProvider implements RetaskContext {

        final Map<Class<?>, Object> map;
        
        public LazyInstanceProvider() {
            map = new HashMap<>();
        }
        
        public void set(Class<?> clazz, Object object) {
            map.put(clazz, object);
        }

        @Override
        public Object getInstance(Class<?> clazz) {
            return map.get(clazz);
        }
    }
}
