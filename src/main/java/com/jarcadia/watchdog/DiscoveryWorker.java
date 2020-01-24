package com.jarcadia.watchdog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.jarcadia.rcommando.CheckedSetMultiFieldResult;
import com.jarcadia.rcommando.RcObject;
import com.jarcadia.rcommando.RcSet;
import com.jarcadia.retask.Retask;
import com.jarcadia.retask.Task;
import com.jarcadia.retask.annontations.RetaskHandler;
import com.jarcadia.retask.annontations.RetaskWorker;

@RetaskWorker
public class DiscoveryWorker {
    
    private final Logger logger = LoggerFactory.getLogger(DiscoveryWorker.class);

    private final PatrolDispatcher dispatcher;

    protected DiscoveryWorker(PatrolDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @RetaskHandler("discover.artifacts")
    public void discoverArtifacts(Retask retask, RcSet artifacts) {
        logger.info("Starting artifact discovery");
        String discoveryId = UUID.randomUUID().toString();
        
        // Ensure agent implementation is ready
        Set<String> missingRoutes = retask.verifyRecruits(Arrays.asList("discover.impl.artifacts"));
        if (!missingRoutes.isEmpty()) {
        	logger.warn("Missing discover.agent.artifacts handler, cannot discover artifacts");
        	throw new DiscoveryException("Undefined implementation for artifact discovery route " + missingRoutes.toString());
        }
        TypeReference<Set<DiscoveredArtifact>> typeRef = new TypeReference<Set<DiscoveredArtifact>>() { };
        Task agentTask = Task.create("discover.impl.artifacts");
        Future<Set<DiscoveredArtifact>> future = retask.call(agentTask, typeRef);
        
        Set<DiscoveredArtifact> discoveredArtifacts = null;
		try {
			discoveredArtifacts = future.get();
		} catch (InterruptedException | ExecutionException e) {
			throw new DiscoveryException("Unexpected exception while discovering artifacts", e);
		}

        DiscoveryResultsMap artifactResults = new DiscoveryResultsMap();
        for (DiscoveredArtifact discovered : discoveredArtifacts) {
            RcObject artifact = artifacts.get(discovered.getId());
            Optional<CheckedSetMultiFieldResult> result = artifact.checkedSet(discovered.getProps());
            if (result.isPresent()) {
                if (result.get().isInsert()) {
                    artifactResults.markAdded(discovered.getType());
                } else {
                    artifactResults.markModified(discovered.getType());
                }
            } else {
                artifactResults.markExisting(discovered.getType());
            }
            artifact.checkedSet("discoveryId", discoveryId);
            artifactResults.markDiscovered(discovered.getType());
        }

        // Remove stale instances
        for (RcObject instance : artifacts) {
            String type = instance.get("type").asString();
            if (!discoveryId.equals(instance.get("discoveryId").asString())) {
                instance.checkedDelete();
                artifactResults.markRemoved(type);
            }
        }

        artifactResults.getTypes().stream().sorted().forEach(type -> {
            if (artifactResults.isRelevant(type)) {
                logger.info("Discovered {} {} artifacts ({})", artifactResults.getDiscovered(type), type, artifactResults.getChangesString(type));
            }
        });
    }

    @RetaskHandler("discover.instances")
    public void discover(Retask retask, RcSet instances, RcSet groups) {
        logger.info("Starting instance discovery");
        String discoveryId = UUID.randomUUID().toString();
        
        Set<String> missingRoutes = retask.verifyRecruits(List.of("discover.impl.instances", "discover.impl.groups"));
        if (!missingRoutes.isEmpty()) {
        	logger.warn("Missing discovery implementation tasks {} cannot discover artifacts", missingRoutes);
        	throw new DiscoveryException("Undefined implementation for instance discovery routes: " + missingRoutes.toString());
        }

        // TODO add atomic boolean lock to only discover one at a time
        
        TypeReference<List<DiscoveredInstance>> ref = new TypeReference<List<DiscoveredInstance>>() { };
        Future<List<DiscoveredInstance>> futureInstances = retask.call(Task.create("discover.impl.instances"), ref);

        List<DiscoveredInstance> discoveredInstances;
		try {
			discoveredInstances = futureInstances.get();
		} catch (InterruptedException | ExecutionException ex) {
			throw new DiscoveryException("Unexpected exception while discovering instances", ex);
		}
        
        Map<String, List<RcObject>> instancesByType = new HashMap<>();
        DiscoveryResultsMap instanceResults = new DiscoveryResultsMap();

        // Process discovered instances
        for (DiscoveredInstance discovered : discoveredInstances) {
            RcObject instance = instances.get(discovered.getId());
            Optional<CheckedSetMultiFieldResult> result = instance.checkedSet(discovered.getProps());
            if (result.isPresent()) {
                if (result.get().isInsert()) {
                    instanceResults.markAdded(discovered.getType());
                } else {
                    instanceResults.markModified(discovered.getType());
                }
                dispatcher.dispatchPatrolsFor(retask, discovered.getType(), instance);
            } else {
                instanceResults.markExisting(discovered.getType());
            }
            instancesByType.computeIfAbsent(discovered.getType(), type -> new ArrayList<>()).add(instance);
            instanceResults.markDiscovered(discovered.getType());
            instance.checkedSet("discoveryId", discoveryId);
        }

        // Remove stale instances
        for (RcObject instance : instances) {
            String type = instance.get("type").asString();
            if (!discoveryId.equals(instance.get("discoveryId").asString())) {
                dispatcher.cancelPatrolsFor(retask, type, instance);
                instance.checkedDelete();
                instanceResults.markRemoved(type);
            }
        }

        // Create group discovery futures
        TypeReference<List<DiscoveredGroup>> listOfGroupsTypeRef = new TypeReference<List<DiscoveredGroup>>() { };
        List<Future<List<DiscoveredGroup>>> groupDiscoveryTasks = instancesByType.entrySet().stream()
        		.map(e -> Task.create("discover.impl.groups")
        				.param("type", e.getKey())
        				.param("instances", e.getValue()))
        		.map(task -> retask.call(task, listOfGroupsTypeRef))
        		.collect(Collectors.toList());
        
        // Combine DiscoveredGroups returned by implementation tasks
        List<DiscoveredGroup> discoveredGroups = new ArrayList<DiscoveredGroup>();
        for (Future<List<DiscoveredGroup>> futureDiscoveredGroups : groupDiscoveryTasks) {
            try {
				discoveredGroups.addAll(futureDiscoveredGroups.get());
			} catch (InterruptedException | ExecutionException ex) {
				throw new DiscoveryException("Unexpected exception while grouping instances", ex);
			}
        }

        DiscoveryResultsMap groupResults = new DiscoveryResultsMap();
        // Process discovered groups
        for (DiscoveredGroup discovered : discoveredGroups) {
        	RcObject group = groups.get(discovered.getId());
            
            Optional<CheckedSetMultiFieldResult> result = group.checkedSet(discovered.getProps());
            if (result.isPresent()) {
//                    dispatcher.dispatchPatrolsFor(discovered.getType(), group);
                if (result.get().isInsert()) {
                    groupResults.markAdded(discovered.getType());
                } else {
                    groupResults.markModified(discovered.getType());
                }
            } else {
                groupResults.markExisting(discovered.getType());
            }
            group.checkedSet("discoveryId", discoveryId);
            groupResults.markDiscovered(discovered.getType());
        }

        // Remove stale groups
        for (RcObject group : groups) {
            if (!discoveryId.equals(group.get("discoveryId").asString())) {
                String type = group.get("type").asString();
                groupResults.markRemoved(type);
//                dispatcher.cancelPatrolsFor(type, instance);
                group.checkedDelete();
            }
        }

        // Set group in each instance that is part of a group
        for (RcObject group : groups) {
            List<String> groupInstanceIds = group.get("instances").asListOf(String.class);
            for (String instanceId : groupInstanceIds) {
                instances.get(instanceId).checkedSet("group", group.getId());
            }
            
        }

        instanceResults.getTypes().stream().sorted().forEach(type -> {
            if (instanceResults.isRelevant(type)) {
                logger.info("Discovered {} {} instances ({})", instanceResults.getDiscovered(type), type, instanceResults.getChangesString(type));
            }
            if (groupResults.isRelevant(type)) {
                logger.info("Discovered {} {} groups ({})", groupResults.getDiscovered(type), type, groupResults.getChangesString(type));
            }
        });
    }

    private class DiscoveryResultsMap {

        private final Map<String, DiscoveryResultCounts> map;
        
        private DiscoveryResultsMap() {
            this.map = new HashMap<>();
        }
        
        private Set<String> getTypes() {
            return map.keySet();
        }
        
        private void markDiscovered(String type) {
            map.computeIfAbsent(type, t -> new DiscoveryResultCounts()).markDiscovered();
        }

        private void markExisting(String type) {
            map.computeIfAbsent(type, t -> new DiscoveryResultCounts()).markExisting();
        }

        private void markAdded(String type) {
            map.computeIfAbsent(type, t -> new DiscoveryResultCounts()).markAdded();
        }

        private void markModified(String type) {
            map.computeIfAbsent(type, t -> new DiscoveryResultCounts()).markModified();
        }

        private void markRemoved(String type) {
            map.computeIfAbsent(type, t -> new DiscoveryResultCounts()).markRemoved();
        }
        
        private boolean isRelevant(String type) {
            if (map.containsKey(type)) {
                return map.get(type).isRelevant();
            } else {
                return false;
            }
        }
        private int getDiscovered(String type) {
            if (map.containsKey(type)) {
                return map.get(type).getDiscovered();
            } else {
                return 0;
            }
        }
        
        private String getChangesString(String type) {
            if (map.containsKey(type)) {
                return map.get(type).getChangesString();
            } else {
                return "";
            }
        }

    }

    private class DiscoveryResultCounts {
        private int discovered;
        private int existing;
        private int added;
        private int modified;
        private int removed;
        
        private DiscoveryResultCounts() {
            this.discovered = 0;
            this.existing = 0;
            this.added = 0;
            this.modified = 0;
            this.removed = 0;
        }
        
        private void markDiscovered() {
            discovered += 1;
        }

        private void markExisting() {
            existing += 1;
        }

        private void markAdded() {
            added += 1;
        }

        private void markModified() {
            modified += 1;
        }

        private void markRemoved() {
            removed += 1;
        }
        
        private boolean isRelevant() {
            return discovered + existing + added + modified + removed > 0;
        }
        
        private int getDiscovered() {
            return discovered;
        }
        
        private String getChangesString() {
            StringJoiner joiner = new StringJoiner("/");
            if (existing > 0) {
                joiner.add(existing + " existing");
            }
            if (added > 0) {
                joiner.add(added + " added");
            }
            if (modified > 0) {
                joiner.add(modified + " modified");
            }
            if (removed > 0) {
                joiner.add(removed + " removed");
            }
            return joiner.toString();
        }
    }
}
