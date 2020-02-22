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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.jarcadia.rcommando.Dao;
import com.jarcadia.rcommando.DaoSet;
import com.jarcadia.rcommando.ProxySet;
import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.rcommando.SetResult;
import com.jarcadia.rcommando.proxy.Internal;
import com.jarcadia.retask.Retask;
import com.jarcadia.retask.Task;
import com.jarcadia.retask.annontations.RetaskHandler;
import com.jarcadia.retask.annontations.RetaskWorker;
import com.jarcadia.watchdog.exception.DiscoveryException;
import com.jarcadia.watchdog.model.Group;
import com.jarcadia.watchdog.model.Instance;

@RetaskWorker
public class DiscoveryWorker {

	private final Logger logger = LoggerFactory.getLogger(DiscoveryWorker.class);

	private final PatrolDispatcher dispatcher;

	protected DiscoveryWorker(PatrolDispatcher dispatcher) {
		this.dispatcher = dispatcher;
	}

	private boolean acquireLock(RedisCommando rcommando, String type) {
		return rcommando.core().getset("discover." + type + ".lock", "locked") == null;
	}

	private void unlock(RedisCommando rcommando, String type) {
		rcommando.core().del("discover." + type + ".lock", "locked");
	}

	@RetaskHandler("discover.artifacts")
	public void discoverArtifacts(RedisCommando rcommando, Retask retask) {
		try {
			if (acquireLock(rcommando, "artifacts")) {
				logger.info("Starting artifact discovery");
				String discoveryId = UUID.randomUUID().toString();

				// Ensure agent implementation is ready
				Set<String> missingRoutes = retask.verifyRecruits(Arrays.asList("discover.artifacts.impl"));
				if (!missingRoutes.isEmpty()) {
					logger.warn("Missing discover.agent.artifacts handler, cannot discover artifacts");
					throw new DiscoveryException("Undefined implementation for artifact discovery route " + missingRoutes.toString());
				}
				TypeReference<Set<DiscoveredArtifact>> typeRef = new TypeReference<Set<DiscoveredArtifact>>() { };
				Task agentTask = Task.create("discover.artifacts.impl");
				Future<Set<DiscoveredArtifact>> future = retask.call(agentTask, typeRef);

				Set<DiscoveredArtifact> discoveredArtifacts = null;
				try {
					discoveredArtifacts = future.get();
				} catch (InterruptedException | ExecutionException e) {
					throw new DiscoveryException("Unexpected exception while discovering artifacts", e);
				}


				// TODO replace daos with proxies
				DaoSet artifactSet = rcommando.getSetOf("artifact");

				StatsByApp artifactResults = new StatsByApp();
				for (DiscoveredArtifact discovered : discoveredArtifacts) {
					String artifactId = discovered.getApp() + "." + discovered.getVersion();
					Dao artifact = artifactSet.get(artifactId);
					Map<String, Object> props = new HashMap<>();
					props.put("app", discovered.getApp());
					props.put("version", discovered.getVersion());
					props.putAll(discovered.getProps());
					Optional<SetResult> result = artifact.set(props);
					artifactResults.record(discovered.getApp(), result);
					artifact.set("discoveryId", discoveryId);
				}

				// Remove stale artifacts
				for (Dao instance : artifactSet) {
					String app = instance.get("app").asString();
					if (!discoveryId.equals(instance.get("discoveryId").asString())) {
						instance.delete();
						artifactResults.recordRemoved(app);
					}
				}

				artifactResults.getApps().stream().sorted().forEach(app -> {
					if (artifactResults.isRelevant(app)) {
						logger.info("Discovered {} {} artifacts ({})", artifactResults.getDiscovered(app), app, artifactResults.getChangesString(app));
					}
				});

			} else {
				logger.info("Artifact discovery already in progress");
			}
		} finally {
			unlock(rcommando, "artifacts");
		}
	}

	@RetaskHandler("discover.instances")
	public void discover(RedisCommando rcommando, Retask retask) {
		try {
			if (acquireLock(rcommando, "instances")) {

				logger.info("Starting instance discovery");
				String discoveryId = UUID.randomUUID().toString();

				// Todo better error messages on missing routes (Inlcude expected annontation)
				Set<String> missingRoutes = retask.verifyRecruits(List.of("discover.instances.impl", "discover.groups.impl"));
				if (!missingRoutes.isEmpty()) {
					logger.warn("Missing discovery implementation tasks {} cannot discover artifacts", missingRoutes);
					throw new DiscoveryException("Undefined implementation for instance discovery routes: " + missingRoutes.toString());
				}

				// TODO add atomic boolean lock to only discover once at a time

				TypeReference<List<DiscoveredInstance>> ref = new TypeReference<List<DiscoveredInstance>>() { };
				Future<List<DiscoveredInstance>> futureInstances = retask.call(Task.create("discover.instances.impl"), ref);

				List<DiscoveredInstance> discoveredInstances;
				try {
					discoveredInstances = futureInstances.get();
				} catch (InterruptedException | ExecutionException ex) {
					throw new DiscoveryException("Unexpected exception while discovering instances", ex);
				}

				// Group the discovered instances by app
				Map<String, List<DiscoveredInstance>> discoveredInstancesByApp = discoveredInstances.stream()
						.collect(Collectors.groupingBy(DiscoveredInstance::getApp));


				// Prepare map to store created proxies
				Map<String, List<DiscoveryInstanceProxy>> instancesByApp = new HashMap<>();

				// Prepare result stats object
				StatsByApp instanceStats = new StatsByApp();

				// Process discovered instances
				ProxySet<DiscoveryInstanceProxy> instanceSet = rcommando.getSetOf("instance", DiscoveryInstanceProxy.class);
				for (Entry<String, List<DiscoveredInstance>> entry : discoveredInstancesByApp.entrySet()) {
					// Prepare a ProxySet specifically for this app
					String app = entry.getKey();

					// Process each discovered instance of this app
					for (DiscoveredInstance discovered : entry.getValue()) {

						// Create a DaoProxy for this instance
						DiscoveryInstanceProxy instance = instanceSet.get(discovered.getApp() + "." + discovered.getId());

						// Set the DaoProxy values with those from the discovered object
						Optional<SetResult> result = instance.getDao().set(discovered.getProps());

						// Record statistics for this app/result
						instanceStats.record(app, result);

						instancesByApp.computeIfAbsent(app, t -> new ArrayList<>()).add(instance);
						instance.setDiscoveryId(discoveryId);
					}
				}

				// Stream all instances of discovered apps (including those that already existed but were not in this discovery)
				instanceSet.stream()
				.forEach(instance -> {
					if (!discoveryId.equals(instance.getDiscoveryId())) {
						instance.delete();
						instanceStats.recordRemoved(instance.getApp());
					}
				});

				// Create group discovery futures
				TypeReference<List<DiscoveredGroup>> listOfGroupsTypeRef = new TypeReference<List<DiscoveredGroup>>() { };
				List<DiscoveredGroupsFuture> groupDiscoveryFutures = instancesByApp.entrySet().stream()
						.map(e -> {
							Task discoveryTask = Task.create("discover.groups.impl")
									.param("app", e.getKey())
									.param("instances", e.getValue());
							Future<List<DiscoveredGroup>> future = retask.call(discoveryTask, listOfGroupsTypeRef);
							return new DiscoveredGroupsFuture(e.getKey(), future);
						}).collect(Collectors.toList());

				// Combine DiscoveredGroups returned by implementation tasks
				Map<String, List<DiscoveredGroup>> discoveredGroupsByApp = new HashMap<>();
				for (DiscoveredGroupsFuture discoveredGroupsFuture : groupDiscoveryFutures) {
					try {
						List<DiscoveredGroup> groups = discoveredGroupsFuture.getFuture().get();
						if (groups.size() > 0) {
							discoveredGroupsByApp.put(discoveredGroupsFuture.getApp(), groups);
						}
					} catch (InterruptedException | ExecutionException ex) {
						throw new DiscoveryException("Unexpected exception while grouping instances", ex);
					}
				}

				StatsByApp groupStats = new StatsByApp();
				ProxySet<DiscoveryGroupProxy> groupSet = rcommando.getSetOf("group", DiscoveryGroupProxy.class);
				for (String app : discoveredGroupsByApp.keySet()) {
					for (DiscoveredGroup discovered : discoveredGroupsByApp.get(app)) {
						DiscoveryGroupProxy group = groupSet.get(app +"." + discovered.getId());
						Map<String, Object> props = new HashMap<>();
						props.put("app", app);
						props.put("instances", discovered.getInstances());
						props.putAll(discovered.getProps());

						Optional<SetResult> result = group.getDao().set(props);
						groupStats.record(app, result);
						if (result.isPresent()) {
							//                    dispatcher.dispatchPatrolsFor(retask, group);
						}
						group.setDiscoveryId(discoveryId);
					}
				}

				// Stream all existing groups (including those that existed previously but weren't discovered just now)
				groupSet.stream()
				.forEach(group -> {
					// Remove group it wasn't discovered just now
					if (!discoveryId.equals(group.getDiscoveryId())) {
						//                    dispatcher.cancelPatrolsForGroup(retask, group);
						groupStats.recordRemoved(group.getApp());
						group.delete();
					}
					// Otherwise update the group field within each instance
					else {
						List<DiscoveryInstanceProxy> groupInstances = group.getInstances();
						for (DiscoveryInstanceProxy instance : groupInstances) {
							instance.setGroup(group);
						}
					}
				});

				// Log results
				instanceStats.getApps().stream().sorted().forEach(app -> {
					if (instanceStats.isRelevant(app)) {
						logger.info("Discovered {} {} instances ({})", instanceStats.getDiscovered(app), app, instanceStats.getChangesString(app));
					}
					if (groupStats.isRelevant(app)) {
						logger.info("Discovered {} {} groups ({})", groupStats.getDiscovered(app), app, groupStats.getChangesString(app));
					}
				});
			} else {
				logger.info("Artifact discovery already in progress");
			}
		} finally {
			unlock(rcommando, "instances");
		}
	}

	private interface DiscoveryInstanceProxy extends Instance {

		@Internal
		public String getDiscoveryId();

		public void setDiscoveryId(@Internal String discoveryId);
		public void setGroup(DiscoveryGroupProxy group);

	}

	private interface DiscoveryGroupProxy extends Group {

		public String getDiscoveryId();
		public List<DiscoveryInstanceProxy> getInstances();

		public void setDiscoveryId(String discoveryId);

	}

	private class DiscoveredGroupsFuture {

		private final String app;
		private final Future<List<DiscoveredGroup>> future;

		private DiscoveredGroupsFuture(String app, Future<List<DiscoveredGroup>> future) {
			this.app = app;
			this.future = future;
		}

		private String getApp() {
			return app;
		}

		private Future<List<DiscoveredGroup>> getFuture() {
			return future;
		}
	}

	private class StatsByApp {

		private final Map<String, Stats> map;

		private StatsByApp() {
			this.map = new HashMap<>();
		}

		private Set<String> getApps() {
			return map.keySet();
		}

		private void record(String app, Optional<SetResult> result) {
			getStats(app).record(result);
		}

		private void recordRemoved(String app) { getStats(app).markRemoved(); }

		private Stats getStats(String app) {
			return map.computeIfAbsent(app, t -> new Stats());
		}

		private boolean isRelevant(String app) {
			if (map.containsKey(app)) {
				return map.get(app).isRelevant();
			} else {
				return false;
			}
		}
		private int getDiscovered(String app) {
			if (map.containsKey(app)) {
				return map.get(app).getDiscovered();
			} else {
				return 0;
			}
		}

		private String getChangesString(String app) {
			if (map.containsKey(app)) {
				return map.get(app).getChangesString();
			} else {
				return "";
			}
		}
	}

	private class Stats {
		private int discovered;
		private int existing;
		private int added;
		private int modified;
		private int removed;

		private Stats() {
			this.discovered = 0;
			this.existing = 0;
			this.added = 0;
			this.modified = 0;
			this.removed = 0;
		}

		private void record(Optional<SetResult> result) {
			discovered += 1;
			if (result.isPresent()) {
				if (result.get().isInsert()) {
					added += 1;
				} else {
					modified += 1;
				}
			} else {
				existing += 1;
			}
		}

		private void markRemoved() {
			removed += 1;
		}

		private boolean isRelevant() {
			return discovered + removed > 0;
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
