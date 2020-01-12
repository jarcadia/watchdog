package com.jarcadia.watchdog;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jarcadia.rcommando.RedisObject;
import com.jarcadia.rcommando.RedisValues;
import com.jarcadia.retask.Retask;
import com.jarcadia.retask.RetaskService;
import com.jarcadia.retask.annontations.RetaskHandler;

import io.netty.util.internal.ThreadLocalRandom;

class PatrolDispatcher {
    
    private final Logger logger = LoggerFactory.getLogger(PatrolDispatcher.class);

    private final RetaskService retaskService;
    private final Map<Patrol, ParamProducer> patrols;

    PatrolDispatcher(RetaskService retaskService, String packageName) {
        this.retaskService = retaskService;
        this.patrols = new HashMap<>();

        Reflections reflections = new Reflections(packageName);
        for (Class<?> clazz : reflections.getTypesAnnotatedWith(WatchdogPatrol.class)) {
            WatchdogPatrol patrolAnnotation = clazz.getAnnotation(WatchdogPatrol.class);

            // Identify the target routingKey
            for (Method method : clazz.getMethods()) {
                RetaskHandler handlerAnnotation = method.getAnnotation(RetaskHandler.class);
                if (handlerAnnotation != null && handlerAnnotation.value().equals(patrolAnnotation.routingKey())) {
                    Patrol patrol = new Patrol(patrolAnnotation.type(), patrolAnnotation.routingKey(), patrolAnnotation.interval(), patrolAnnotation.unit());
                    List<Param> params = Stream.of(method.getParameters())
                            .filter(p -> Stream.of(patrolAnnotation.properties()).anyMatch(prop -> prop.equals(p.getName())))
                            .map(p -> new Param(p.getType(), p.getName()))
                            .collect(Collectors.toList());
                    patrols.put(patrol, new ParamProducer(params));
                }
            }
        }
    }

    public void dispatchPatrolsFor(String type, RedisObject object) {
        for (Patrol patrol : patrols.keySet()) {
            if (patrol.getType().equals(type) || patrol.getType().equals("*")) {
                String recurKey = patrol.getRoutingKey() + "." + object.getId();
                long delayMillis = ThreadLocalRandom.current().nextLong(patrol.getUnit().toMillis(patrol.getInterval()));
                Retask task = Retask.create(patrol.getRoutingKey())
                        .in(delayMillis, TimeUnit.MILLISECONDS)
                        .recurEvery(recurKey, patrol.getInterval(), patrol.getUnit());
                patrols.get(patrol).prepareTaskParameters(task, object);
                retaskService.submit(task);
                logger.info("Dispatched {} for {} {} in {}", patrol.getRoutingKey(), type, object.getId(), delayMillis);
            }
        }
    }

    public void cancelPatrolsFor(String type, RedisObject object) {
        for (Patrol patrol : patrols.keySet()) {
            if (patrol.getType().equals(type) || patrol.getType().equals("*")) {
                String recurKey = patrol.getRoutingKey() + "." + object.getId();
                retaskService.revokeAuthority(recurKey);
                logger.info("Canceled {} for {} {}", patrol.getRoutingKey(), type, object.getId());
            }
        }
    }

    private class ParamProducer {
        private final List<Param>  params;
        private final String[] paramNames;
        
        protected ParamProducer(List<Param> params) {
            this.params = params;
            this.paramNames = params.stream().map(Param::getName).collect(Collectors.toList()).toArray(new String[0]);
        }
        
        protected void prepareTaskParameters(Retask task, RedisObject obj) {
            task.param("object", obj);
            RedisValues values = obj.get(paramNames);
            for (Param param : params) {
                task.param(param.getName(), values.next().as(param.getType()));
            }
        }
    }

    private class Param {

        private final Class<?> type;
        private final String name;

        public Param(Class<?> type, String name) {
            super();
            this.type = type;
            this.name = name;
        }
        
        public Class<?> getType() {
            return type;
        }
        
        public String getName() {
            return name;
        }
    }
}
