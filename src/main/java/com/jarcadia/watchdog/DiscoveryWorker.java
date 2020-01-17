package com.jarcadia.watchdog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jarcadia.rcommando.CheckedSetMultiFieldResult;
import com.jarcadia.rcommando.RedisMap;
import com.jarcadia.rcommando.RedisObject;
import com.jarcadia.retask.Retask;
import com.jarcadia.retask.annontations.RetaskHandler;
import com.jarcadia.retask.annontations.RetaskWorker;

@RetaskWorker
public class DiscoveryWorker {
    
    private final Logger logger = LoggerFactory.getLogger(DiscoveryWorker.class);

    private final DiscoveryAgent agent;
    private final PatrolDispatcher dispatcher;

    protected DiscoveryWorker(DiscoveryAgent agent, PatrolDispatcher dispatcher) {
        this.agent = agent;
        this.dispatcher = dispatcher;
    }

    @RetaskHandler("discover.artifacts")
    public void discoverArtifacts(RedisMap artifacts) {
        logger.info("Starting artifact discovery");
        String discoveryId = UUID.randomUUID().toString();
        
        Set<DiscoveredArtifact> discoveredArtifacts = agent.discoverArtifacts();
        DiscoveryResultsMap artifactResults = new DiscoveryResultsMap();
        for (DiscoveredArtifact discovered : discoveredArtifacts) {
            RedisObject artifact = artifacts.get(discovered.getId());
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
        for (RedisObject instance : artifacts) {
            String type = instance.get("type").asString();
            if (!discoveryId.equals(instance.get("discoveryId").asString())) {
                dispatcher.cancelPatrolsFor(type, instance);
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
    public void discover(RedisMap instances, RedisMap groups) throws Exception {
        
        logger.info("Starting instance discovery");
        String discoveryId = UUID.randomUUID().toString();
        
        // TODO add atomic boolean lock to only discover one at a time
        
        Collection<DiscoveredInstance> discoveredInstances = agent.discoverInstances();
        Map<String, List<RedisObject>> instancesByType = new HashMap<>();
        DiscoveryResultsMap instanceResults = new DiscoveryResultsMap();

        // Process discovered instances
        for (DiscoveredInstance discovered : discoveredInstances) {
            RedisObject instance = instances.get(discovered.getId());
            Optional<CheckedSetMultiFieldResult> result = instance.checkedSet(discovered.getProps());
            if (result.isPresent()) {
                if (result.get().isInsert()) {
                    instanceResults.markAdded(discovered.getType());
                } else {
                    instanceResults.markModified(discovered.getType());
                }
                dispatcher.dispatchPatrolsFor(discovered.getType(), instance);
            } else {
                instanceResults.markExisting(discovered.getType());
            }
            instancesByType.computeIfAbsent(discovered.getType(), type -> new ArrayList<>()).add(instance);
            instanceResults.markDiscovered(discovered.getType());
            instance.checkedSet("discoveryId", discoveryId);
        }

        // Remove stale instances
        for (RedisObject instance : instances) {
            String type = instance.get("type").asString();
            if (!discoveryId.equals(instance.get("discoveryId").asString())) {
                dispatcher.cancelPatrolsFor(type, instance);
                instance.checkedDelete();
                instanceResults.markRemoved(type);
            }
        }

        // Discover groups based on discovered instances
        List<DiscoveredGroup> discoveredGroups = new ArrayList<DiscoveredGroup>();
        for (Entry<String, List<RedisObject>> entry : instancesByType.entrySet()) {
            discoveredGroups.addAll(agent.groupInstances(entry.getKey(), entry.getValue()));
        }

        DiscoveryResultsMap groupResults = new DiscoveryResultsMap();
        // Process discovered groups
        for (DiscoveredGroup discovered : discoveredGroups) {
            RedisObject group = groups.get(discovered.getId());
            
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
        for (RedisObject group : groups) {
            if (!discoveryId.equals(group.get("discoveryId").asString())) {
                String type = group.get("type").asString();
                groupResults.markRemoved(type);
//                dispatcher.cancelPatrolsFor(type, instance);
                group.checkedDelete();
            }
        }

        // Set group in each instance that is part of a group
        for (RedisObject group : groups) {
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
