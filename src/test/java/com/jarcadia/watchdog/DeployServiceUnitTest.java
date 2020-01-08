package com.jarcadia.watchdog;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.rcommando.RedisMap;
import com.jarcadia.rcommando.RedisObject;
import com.jarcadia.retask.Retask;
import com.jarcadia.retask.RetaskInit;
import com.jarcadia.retask.RetaskRecruiter;
import com.jarcadia.retask.RetaskService;
import com.jarcadia.retask.RetaskWorkerInstanceProvider;
import com.jarcadia.retask.annontations.RetaskChangeHandler;
import com.jarcadia.retask.annontations.RetaskDeleteHandler;
import com.jarcadia.retask.annontations.RetaskInsertHandler;
import com.jarcadia.retask.annontations.RetaskWorker;
import com.jarcadia.watchdog.deploy.DeployState;
import com.jarcadia.watchdog.deploy.DeploymentAgent;
import com.jarcadia.watchdog.deploy.DeploymentWorker;
import com.jarcadia.watchdog.distribute.DistributionAgent;
import com.jarcadia.watchdog.distribute.DistributionState;
import com.jarcadia.watchdog.distribute.DistributionWorker;

import io.lettuce.core.RedisClient;

@RetaskWorker
public class DeployServiceUnitTest {

    private final Logger logger = LoggerFactory.getLogger(DeployServiceUnitTest.class);
    
    static RedisClient redisClient;
    static RetaskRecruiter recruiter;
    static Random random;

    @BeforeAll
    public static void setup() {
        redisClient = RedisClient.create("redis://localhost/15");
        
        recruiter = new RetaskRecruiter();
        recruiter.recruitFromClass(DeploymentWorker.class);
        recruiter.recruitFromClass(DistributionWorker.class);
        recruiter.recruitFromClass(InstanceStateRecorder.class);
        recruiter.recruitFromClass(DeploymentStateRecorder.class);
        recruiter.recruitFromClass(DeploymentAgent.class);
        random = new Random();
    }

    @BeforeEach
    public void flush() {
    }

    @Test
    public void testSingleInstanceDeployment() throws Exception {
        RedisCommando rcommando = RedisCommando.create(redisClient, new ObjectMapper());
        rcommando.core().flushdb();
        LazyInstanceProvider provider = new LazyInstanceProvider();
        RetaskService retaskService = RetaskInit.init(redisClient, rcommando, recruiter, provider);

        // Setup data
        RedisMap instances = rcommando.getMap("instances");
        instances.get("inst").set("type", "webserver", "group", "group", "host", "web01", "port", 8080, "state", InstanceState.ENABLED);
//        RedisMap groups = rcommando.getMap("groups");
//        groups.get("group").set("instances", Arrays.asList("inst"));

        // Setup spied Distribution Agent
        DistributionAgent distributionAgent = Mockito.spy(new TestDistributionAgent());

        // Setup spied Deployment Agent
        DeploymentAgent deploymentAgent = Mockito.spy(new TestDeploymentAgent());

        // Setup InstateStateRecorder for assertions
        InstanceStateRecorder stateRecorder = new InstanceStateRecorder();
        provider.set(InstanceStateRecorder.class, stateRecorder);

        // Setup DeploymentStateRecorder for assertions
        DeploymentStateRecorder deployStateRecorder = new DeploymentStateRecorder();
        provider.set(DeploymentStateRecorder.class, deployStateRecorder);

        // Create Deploy Service for test
        DeploymentWorker deployService = new DeploymentWorker(rcommando, deploymentAgent);
        provider.set(DeploymentWorker.class, deployService);

        // Create Distribution Worker for test
        DistributionWorker distWorker = new DistributionWorker(rcommando, distributionAgent);
        provider.set(DistributionWorker.class, distWorker);

        // Submit task to start deployment
        Path localPath = Paths.get("/data/releases/webserver/1.0/webserver.jar");
        retaskService.start();
        retaskService.submit(Retask.create("deploy")
                .param("localPath", localPath)
                .param("instanceIds", Arrays.asList("inst"))
                .param("version", "1.0")
                .param("hash", "abc123"));

        // Wait for the deployment to complete
        deployStateRecorder.awaitCompletion(1, 1, TimeUnit.HOURS);

        // Verify the instances deploy states progressed as expected
        Assertions.assertIterableEquals(expectedDeployStates(), deployStateRecorder.getStates("inst"));

        // Verify the instances states progressed as expected
        Assertions.assertIterableEquals(expectedInstanceStates(), stateRecorder.getStates("inst"));

        // Verify the distribution agent callbacks where invoked in order
        verifyDistAgent(distributionAgent, "web01", localPath);

        // Verify the deployment agent spy callbacks were invoked in order
        verifyDeployAgent(deploymentAgent, "instances", "inst");

        retaskService.shutdown(1, TimeUnit.SECONDS);
    }

    @Test
    public void testTwoInstanceDeployment() throws Exception {
        RedisCommando rcommando = RedisCommando.create(redisClient, new ObjectMapper());
        rcommando.core().flushdb();
        LazyInstanceProvider provider = new LazyInstanceProvider();
        RetaskService retaskService = RetaskInit.init(redisClient, rcommando, recruiter, provider);

        // Setup data
        RedisMap instances = rcommando.getMap("instances");
        instances.get("inst1").set("type", "webserver", "group", "group", "host", "web01", "port", 8080, "state", InstanceState.ENABLED);
        instances.get("inst2").set("type", "webserver", "group", "group", "host", "web02", "port", 8080, "state", InstanceState.ENABLED);
        RedisMap groups = rcommando.getMap("groups");
        groups.get("group").set("instances", Arrays.asList("inst1", "inst2"));

        // Setup spied Distribution Agent
        DistributionAgent distributionAgent = Mockito.spy(new TestDistributionAgent());

        // Setup spied Deployment Agent
        DeploymentAgent deploymentAgent = Mockito.spy(new TestDeploymentAgent());

        // Setup InstateStateRecorder for assertions
        InstanceStateRecorder stateRecorder = new InstanceStateRecorder();
        provider.set(InstanceStateRecorder.class, stateRecorder);

        // Setup DeploymentStateRecorder for assertions
        DeploymentStateRecorder deployStateRecorder = new DeploymentStateRecorder();
        provider.set(DeploymentStateRecorder.class, deployStateRecorder);

        // Create Deploy Service for test
        DeploymentWorker deployService = new DeploymentWorker(rcommando, deploymentAgent);
        provider.set(DeploymentWorker.class, deployService);

        // Create Distribution Worker for test
        DistributionWorker distWorker = new DistributionWorker(rcommando, distributionAgent);
        provider.set(DistributionWorker.class, distWorker);

        // Submit task to start deployment
        Path localPath = Paths.get("/data/releases/webserver/1.0/webserver.jar");
        retaskService.start();
        retaskService.submit(Retask.create("deploy").param("instanceIds", Arrays.asList("inst1", "inst2")).param("version", "1.0").param("localPath", localPath));

        // Wait for the deployment to complete
        deployStateRecorder.awaitCompletion(1, 1, TimeUnit.SECONDS);

        // Verify the instances deploy states progressed as expected
        Assertions.assertIterableEquals(expectedDeployStates(), deployStateRecorder.getStates("inst1"));
        Assertions.assertIterableEquals(expectedDeployStates(), deployStateRecorder.getStates("inst2"));

        // Verify the instances states progressed as expected
        Assertions.assertIterableEquals(expectedInstanceStates(), stateRecorder.getStates("inst1"));
        Assertions.assertIterableEquals(expectedInstanceStates(), stateRecorder.getStates("inst2"));

        // Verify the distribution agent callbacks where invoked in order
        verifyDistAgent(distributionAgent, "web01", localPath);
        verifyDistAgent(distributionAgent, "web02", localPath);

        // Verify the deployment agent spy callbacks were invoked in order
        verifyDeployAgent(deploymentAgent, "instances", "inst1");
        verifyDeployAgent(deploymentAgent, "instances", "inst2");

        retaskService.shutdown(1, TimeUnit.SECONDS);
    }

    @Test
    public void testLargeMultiDeployment() throws Exception {
        RedisCommando rcommando = RedisCommando.create(redisClient, new ObjectMapper());
        rcommando.core().flushdb();
        LazyInstanceProvider provider = new LazyInstanceProvider();
        RetaskService retaskService = RetaskInit.init(redisClient, rcommando, recruiter, provider);
        
        int numGroups = 50;
        String[][] hosts = {{"web01", "web02"}, {"web03", "web04"}, {"web05", "web06"}, {"web07", "web08"}};
        RedisMap instances = rcommando.getMap("instances");
        RedisMap groups = rcommando.getMap("groups");
        List<String> instanceIds = new ArrayList<>();
        for (int i=0; i<numGroups; i++) {
            String groupId = "group" + i;
            String inst1Id = groupId + "-" + "inst1";
            String inst2Id = groupId + "-" + "inst2";

            instanceIds.add(inst1Id);
            instanceIds.add(inst2Id);
            String[] groupHosts = hosts[random.nextInt(hosts.length)];
            instances.get(inst1Id).set("type", "webserver", "group", groupId, "host", groupHosts[0], "port", 8080 + i, "state", InstanceState.ENABLED);
            instances.get(inst2Id).set("type", "webserver", "group", groupId, "host", groupHosts[1], "port", 8080 + i, "state", InstanceState.ENABLED);
            groups.get(groupId).set("instances", Arrays.asList(inst1Id, inst2Id));
        }

        // Setup spied Deployment Agent
        DeploymentAgent deploymentAgent = Mockito.spy(new TestDeploymentAgent());
        provider.set(DeploymentAgent.class, deploymentAgent);
        
        // Setup spied Distribution Agent
        DistributionAgent distributionAgent = Mockito.spy(new TestDistributionAgent());
        provider.set(DistributionAgent.class, distributionAgent);

        // Setup InstateStateRecorder for assertions
        InstanceStateRecorder stateRecorder = new InstanceStateRecorder();
        provider.set(InstanceStateRecorder.class, stateRecorder);

        // Setup DeploymentStateRecorder for assertions
        DeploymentStateRecorder deployStateRecorder = new DeploymentStateRecorder();
        provider.set(DeploymentStateRecorder.class, deployStateRecorder);

        // Create Deploy Service for test
        DeploymentWorker deployService = new DeploymentWorker(rcommando, deploymentAgent);
        provider.set(DeploymentWorker.class, deployService);

        // Create Distribution Worker for test
        DistributionWorker distWorker = new DistributionWorker(rcommando, distributionAgent);
        provider.set(DistributionWorker.class, distWorker);

        // Submit task to start deployment
        Path localPath = Paths.get("/data/releases/webserver/1.0/webserver.jar");
        retaskService.start();
        retaskService.submit(Retask.create("deploy").param("instanceIds", instanceIds).param("version", "1.0").param("localPath", localPath));

        // Wait for the deployment to complete
        deployStateRecorder.awaitCompletion(numGroups, 10, TimeUnit.SECONDS);

        for (String instanceId : instanceIds) {
            Assertions.assertIterableEquals(expectedDeployStates(), deployStateRecorder.getStates(instanceId));
            verifyDeployAgent(deploymentAgent, "instances", instanceId);
        }
        retaskService.shutdown(10, TimeUnit.SECONDS);
    }
    

    private List<InstanceState> expectedInstanceStates() {
        return Arrays.asList(InstanceState.DISABLED, InstanceState.DOWN, InstanceState.DISABLED, InstanceState.ENABLED);
    }

    private List<DeployState> expectedDeployStates() {
        return Arrays.asList(DeployState.Waiting, DeployState.Ready, DeployState.PendingDisable, DeployState.Disabling,
                DeployState.PendingStop, DeployState.Stopping, DeployState.PendingUpgrade, DeployState.Upgrading, DeployState.Upgraded,
                DeployState.PendingStart, DeployState.Starting, DeployState.PendingEnable, DeployState.Enabling);
    }

    private void verifyDistAgent(DistributionAgent agent, String host, Path localPath) {
//        InOrder distVerifier = Mockito.inOrder(agent);
//        distVerifier.verify(agent).getLocalPath("webserver", "1.0");
//        distVerifier.verify(agent).hash(localPath);
//        distVerifier.verify(agent).transfer(Mockito.eq("web01"), Mockito.eq(localPath), Mockito.any());
//        distVerifier.verify(agent).verify(Mockito.eq("web01"), Mockito.any(), Mockito.any());
    }

    private void verifyDeployAgent(DeploymentAgent agent, String mapKey, String id) throws Exception {
        InOrder depVerifer = Mockito.inOrder(agent);
        RedisObject expectedInstance = new RedisObject(null, null, mapKey, id);
        depVerifer.verify(agent, Mockito.times(1)).disable(expectedInstance);
        depVerifer.verify(agent, Mockito.times(1)).stop(expectedInstance);
        Mockito.verify(agent, Mockito.times(1)).upgrade(Mockito.eq(expectedInstance), Mockito.any());
        depVerifer.verify(agent, Mockito.times(1)).start(expectedInstance);
        depVerifer.verify(agent, Mockito.times(1)).enable(expectedInstance);
    }
    
    public class TestDistributionAgent implements DistributionAgent {

        @Override
        public void transfer(RedisObject distribution, String host, Path localPath, Path remotePath) {
            distribution.checkedSet("state", DistributionState.Transferred);
        }

        @Override
        public void verify(RedisObject distribution, String host, Path remotePath, String hash) {
            distribution.checkedSet("state", DistributionState.Verified);
        }
        
    }

    public class TestDeploymentAgent implements DeploymentAgent {
        
        public TestDeploymentAgent() {
        }

        public void chooseNext(RedisObject deployment, List<RedisObject> instances) {
            instances.get(0).checkedSet("deploymentState", DeployState.Ready);
        }

        public void disable(RedisObject instance) {
            logger.info("Disabling {}", instance.getId());
            instance.checkedSet("state", InstanceState.DISABLED);
        }

        public void stop(RedisObject instance) {
            logger.info("Stopping {}", instance.getId());
            instance.checkedSet("state", InstanceState.DOWN);
        }

        public void upgrade(RedisObject instance) {
            logger.info("Upgrading {}", instance.getId());
        }

        public void start(RedisObject instance) {
            logger.info("Starting {}", instance.getId());
            instance.checkedSet("state", InstanceState.DISABLED);
        }

        public void enable(RedisObject instance) {
            logger.info("Enabling {}", instance.getId());
            instance.checkedSet("state", InstanceState.ENABLED);
        }

        public void upgrade(RedisObject instance, RedisObject distribution) {
            logger.info("Upgrading {}", instance.getId());
            instance.checkedSet("deploymentState", DeployState.Upgraded);
        }
    }

    @RetaskWorker
    public class InstanceStateRecorder {

        final Map<String, List<InstanceState>> stateMap;

        public InstanceStateRecorder() {
            stateMap = Collections.synchronizedMap(new HashMap<>());
        }

        @RetaskChangeHandler(mapKey = "instances", field = "state")
        public void changeState(RedisObject instance, InstanceState before, InstanceState after) {
            stateMap.computeIfAbsent(instance.getId(), id -> Collections.synchronizedList(new ArrayList<>())).add(after);
            logger.info("Recording state change for {}: {} -> {}", instance.getId(), before, after);
        }

        public List<InstanceState> getStates(String instanceId) {
            return stateMap.get(instanceId);
        }
    }

    @RetaskWorker
    public class DeploymentStateRecorder {

        final Map<String, List<DeployState>> stateMap;
        final Map<String, CompletableFuture<Void>> futures;

        public DeploymentStateRecorder() {
            stateMap = Collections.synchronizedMap(new HashMap<>());
            futures = Collections.synchronizedMap(new HashMap<>());
        }
        
        @RetaskInsertHandler("deployments")
        public void deploymentInserted(RedisObject deployment) {
            futures.put(deployment.getId(), new CompletableFuture<>());
        }

        @RetaskDeleteHandler("deployments")
        public void deploymentDeleted(RedisObject deployment) {
            futures.get(deployment.getId()).complete(null);
        }

        @RetaskChangeHandler(mapKey = "instances", field = "deploymentState")
        public void changeState(RedisObject instance, DeployState before, DeployState after) {
            synchronized (this) {
                stateMap.computeIfAbsent(instance.getId(), id -> Collections.synchronizedList(new ArrayList<>())).add(after);
            }
        }

        public void awaitCompletion(int expectedDeployments, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            while (futures.size() < expectedDeployments) {
                Thread.sleep(10);
            }
            for (CompletableFuture<Void> future : futures.values()) {
                future.get(timeout, unit);
            }
        }

        public Set<String> getIds() {
            return stateMap.keySet();
        }

        public List<DeployState> getStates(String deploymentId) {
            return stateMap.get(deploymentId);
        }
    }

    static class LazyInstanceProvider implements RetaskWorkerInstanceProvider {

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
