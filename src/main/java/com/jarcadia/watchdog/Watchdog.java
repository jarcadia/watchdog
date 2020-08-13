package com.jarcadia.watchdog;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.jarcadia.rcommando.Dao;
import com.jarcadia.rcommando.RedisCommando;
import com.jarcadia.rcommando.proxy.Proxy;
import com.jarcadia.retask.Retask;
import com.jarcadia.retask.RetaskManager;
import com.jarcadia.retask.RetaskRecruiter;
import com.jarcadia.watchdog.annontation.WatchdogArtifactDiscoveryHandler;
import com.jarcadia.watchdog.annontation.WatchdogGroupDiscoveryHandler;
import com.jarcadia.watchdog.annontation.WatchdogGroupPatrol;
import com.jarcadia.watchdog.annontation.WatchdogInstanceDiscoveryHandler;
import com.jarcadia.watchdog.annontation.WatchdogInstancePatrol;

import io.lettuce.core.RedisClient;

public class Watchdog {
    
    public static WatchdogManager init(RedisClient redisClient, RedisCommando rcommando, String packageName) {

		// Setup NotificationService
		NotificationService notificationService = new NotificationService(rcommando);

		// Setup rcommando internal object mapper to correctly serialize/deserialize Discovery models
    	registerDiscoveredInstanceModule(rcommando.getObjectMapper());
    	registerDiscoveredGroupModule(rcommando.getObjectMapper());
    	registerDiscoveredArtifactModule(rcommando.getObjectMapper());

    	// Setup recruitment from source package and internal package
        RetaskRecruiter recruiter = new RetaskRecruiter();
        recruiter.recruitFromPackage(packageName);
        recruiter.recruitFromPackage("com.jarcadia.watchdog");

    	// Setup recruitment of Watchdog annontations
        recruiter.registerTaskHandlerAnnontation(WatchdogInstanceDiscoveryHandler.class,
        		(clazz, method, annotation) -> "discover.instances.impl");
        recruiter.registerTaskHandlerAnnontation(WatchdogGroupDiscoveryHandler.class,
        		(clazz, method, annotation) -> "discover.groups.impl");
        recruiter.registerTaskHandlerAnnontation(WatchdogArtifactDiscoveryHandler.class,
        		(clazz, method, annotation) -> "discover.artifacts.impl");
        recruiter.registerTaskHandlerAnnontation(WatchdogInstancePatrol.class,
       		(clazz, method, annotation) -> "patrol.instance." + clazz.getSimpleName() + "." + method.getName());
        recruiter.registerTaskHandlerAnnontation(WatchdogGroupPatrol.class,
        		(clazz, method, annotation) -> "patrol.group." + clazz.getSimpleName() + "." + method.getName());
        
        RetaskManager retaskManager = Retask.init(redisClient, rcommando, recruiter);

        // Setup dispatcher and discovery worker
        PatrolDispatcher dispatcher = new PatrolDispatcher(rcommando,
        		retaskManager.getHandlersByAnnotation(WatchdogInstancePatrol.class),
        		retaskManager.getHandlersByAnnotation(WatchdogGroupPatrol.class));
        DiscoveryWorker discoveryWorker = new DiscoveryWorker(dispatcher);
        
        WatchdogMonitoringFactory monitoringFactory = new WatchdogMonitoringFactory(rcommando);
        for (Class<? extends Proxy> proxyClass : retaskManager.getDaoProxies()) {
        	monitoringFactory.setupMonitoring(proxyClass);
        }

        // Setup deployment worker
        DeploymentWorker deploymentWorker = new DeploymentWorker(rcommando, notificationService);
        
        // Register instances 
        retaskManager.addWorker(dispatcher, discoveryWorker, deploymentWorker);

        return new WatchdogManager(retaskManager, notificationService);
    }

    private static void registerDiscoveredInstanceModule(ObjectMapper mapper) {
        SimpleModule module = new SimpleModule();
        module.addSerializer(new DiscoveredInstanceSerializer());
        JavaType mapType = mapper.getTypeFactory().constructMapLikeType(Map.class, String.class, Object.class);
        module.addDeserializer(DiscoveredInstance.class, new DiscoveredInstanceDeserializer(mapType));
        mapper.registerModule(module);
    }
    
    private static void registerDiscoveredGroupModule(ObjectMapper mapper) {
        SimpleModule module = new SimpleModule();
        module.addSerializer(new DiscoveredGroupSerializer());
        JavaType mapType = mapper.getTypeFactory().constructMapLikeType(Map.class, String.class, Object.class);
        JavaType listType = mapper.getTypeFactory().constructCollectionLikeType(List.class, Dao.class);
        module.addDeserializer(DiscoveredGroup.class, new DiscoveredGroupDeserializer(mapType, listType));
        mapper.registerModule(module);
    }
    
    private static void registerDiscoveredArtifactModule(ObjectMapper mapper) {
        SimpleModule module = new SimpleModule();
        module.addSerializer(new DiscoveredArtifactSerializer());
        JavaType mapType = mapper.getTypeFactory().constructMapLikeType(Map.class, String.class, Object.class);
        module.addDeserializer(DiscoveredArtifact.class, new DiscoveredArtifactDeserializer(mapType));
        mapper.registerModule(module);
    }
    
    private static class DiscoveredInstanceSerializer extends StdSerializer<DiscoveredInstance> {
		
	    public DiscoveredInstanceSerializer() {
	        super(DiscoveredInstance.class);
	    }
	 
		@Override
		public void serialize(DiscoveredInstance value, JsonGenerator gen, SerializerProvider provider) throws IOException {
			 gen.writeStartObject();
			 gen.writeStringField("type", value.getApp());
			 gen.writeStringField("id", value.getId());
			 gen.writeObjectField("props", value.getProps());
		     gen.writeEndObject();
		}
	}
	
	private static class DiscoveredInstanceDeserializer extends StdDeserializer<DiscoveredInstance> {
		
		private final JavaType mapType;
		
		public DiscoveredInstanceDeserializer(JavaType mapType) {
			super(DiscoveredInstance.class);
			this.mapType = mapType;
	    }
	 
	    @Override
	    public DiscoveredInstance deserialize(JsonParser parser, DeserializationContext deserializer) throws IOException {
	    	JsonNode node = parser.readValueAsTree();
	    	final String type = node.get("type").asText();
	    	final String id = node.get("id").asText();
	    	JsonParser propsParser = node.get("props").traverse();
	    	propsParser.nextToken();
	    	final Map<String, Object> props = deserializer.readValue(propsParser, mapType);
	    	return new DiscoveredInstance(type, id, props);
	    }
	}
	
    private static class DiscoveredGroupSerializer extends StdSerializer<DiscoveredGroup> {
		
	    public DiscoveredGroupSerializer() {
	        super(DiscoveredGroup.class);
	    }
	 
		@Override
		public void serialize(DiscoveredGroup value, JsonGenerator gen, SerializerProvider provider) throws IOException {
			 gen.writeStartObject();
			 gen.writeStringField("id", value.getId());
			 gen.writeObjectField("instances", value.getInstances());
			 gen.writeObjectField("props", value.getProps());
		     gen.writeEndObject();
		}
	}
	
	private static class DiscoveredGroupDeserializer extends StdDeserializer<DiscoveredGroup> {
		
		private final JavaType listType;
		private final JavaType mapType;

		public DiscoveredGroupDeserializer(JavaType mapType, JavaType listType) {
			super(DiscoveredGroup.class);
			this.mapType = mapType;
			this.listType = listType;
	    }
	 
	    @Override
	    public DiscoveredGroup deserialize(JsonParser parser, DeserializationContext deserializer) throws IOException {
	    	JsonNode node = parser.readValueAsTree();
	    	final String id = node.get("id").asText();
	    	JsonParser instancesParser = node.get("instances").traverse(deserializer.getParser().getCodec());
	    	instancesParser.nextToken();
	    	final List<Dao> instances = deserializer.readValue(instancesParser, listType);
	    	JsonParser propsParser = node.get("props").traverse();
	    	propsParser.nextToken();
	    	final Map<String, Object> props = deserializer.readValue(propsParser, mapType);
	    	return new DiscoveredGroup(id, instances, props);
	    }
	}
	
    private static class DiscoveredArtifactSerializer extends StdSerializer<DiscoveredArtifact> {
		
	    public DiscoveredArtifactSerializer() {
	        super(DiscoveredArtifact.class);
	    }
	 
		@Override
		public void serialize(DiscoveredArtifact value, JsonGenerator gen, SerializerProvider provider) throws IOException {
			 gen.writeStartObject();
			 gen.writeStringField("type", value.getApp());
			 gen.writeObjectField("version", value.getVersion());
			 gen.writeObjectField("props", value.getProps());
		     gen.writeEndObject();
		}
	}
	
	private static class DiscoveredArtifactDeserializer extends StdDeserializer<DiscoveredArtifact> {
		
		private final JavaType mapType;

		public DiscoveredArtifactDeserializer(JavaType mapType) {
			super(DiscoveredArtifact.class);
			this.mapType = mapType;
	    }
	 
	    @Override
	    public DiscoveredArtifact deserialize(JsonParser parser, DeserializationContext deserializer) throws IOException {
	    	JsonNode node = parser.readValueAsTree();
	    	final String type = node.get("type").asText();
	    	final String version = node.get("version").asText();
	    	JsonParser propsParser = node.get("props").traverse();
	    	propsParser.nextToken();
	    	final Map<String, Object> props = deserializer.readValue(propsParser, mapType);
	    	return new DiscoveredArtifact(type, version, props);
	    }
	}
}
