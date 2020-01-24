package com.jarcadia.watchdog;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jarcadia.rcommando.RcObject;
import com.jarcadia.rcommando.RcValues;
import com.jarcadia.retask.HandlerMethod;
import com.jarcadia.retask.Retask;
import com.jarcadia.retask.Task;

import io.netty.util.internal.ThreadLocalRandom;

class PatrolDispatcher {
    
    private final Logger logger = LoggerFactory.getLogger(PatrolDispatcher.class);

    private final List<Patrol> patrols;

    protected PatrolDispatcher(ObjectMapper objectMapper, List<HandlerMethod<WatchdogPatrol>> patrolHandlers) {
        List<Patrol> list = new LinkedList<>();
        for (HandlerMethod<WatchdogPatrol> handler : patrolHandlers) {
        	WatchdogPatrol annontation = handler.getAnnontation();
        	List<PatrolParam> params = Stream.of(handler.getMethod().getParameters())
                    .filter(p -> Stream.of(annontation.properties()).anyMatch(prop -> prop.equals(p.getName())))
                    .map(p -> new PatrolParam(p.getName(), objectMapper.constructType(p.getParameterizedType())))
                    .collect(Collectors.toList());
        	PatrolParamProducer paramProducer = new PatrolParamProducer(params);
            list.add(new Patrol(annontation.type(), handler.getRoutingKey(),
            		annontation.interval(), annontation.unit(), paramProducer));

        }
        this.patrols = List.copyOf(list);
    }

    protected void dispatchPatrolsFor(Retask retask, String type, RcObject instance) {
        for (Patrol patrol : patrols) {
            if (patrol.getType().equals(type) || patrol.getType().equals("*")) {
                String recurKey = patrol.getRoutingKey() + "." + instance.getId();
                long delayMillis = ThreadLocalRandom.current().nextLong(patrol.getUnit().toMillis(patrol.getInterval()));
                Task task = Task.create(patrol.getRoutingKey())
                        .in(delayMillis, TimeUnit.MILLISECONDS)
                        .recurEvery(recurKey, patrol.getInterval(), patrol.getUnit());
                patrol.prepareTaskParameters(task, instance);
                retask.submit(task);
                logger.info("Dispatched patrol {} for {} {} in {}", patrol.getRoutingKey(), type, instance.getId(), delayMillis);
            }
        }
    }

    protected void cancelPatrolsFor(Retask retask, String type, RcObject instance) {
        for (Patrol patrol : patrols) {
            if (patrol.getType().equals(type) || patrol.getType().equals("*")) {
                String recurKey = patrol.getRoutingKey() + "." + instance.getId();
                retask.revokeAuthority(recurKey);
                logger.info("Canceled patrol {} for {} {}", patrol.getRoutingKey(), type, instance.getId());
            }
        }
    }
    
    private class Patrol {

        private final String type;
        private final String routingKey;
        private final long interval;
        private final TimeUnit unit;
        private final PatrolParamProducer paramProducer;

        protected Patrol(String type, String routingKey, long interval, TimeUnit unit, PatrolParamProducer paramProducer) {
            this.type = type;
            this.routingKey = routingKey;
            this.interval = interval;
            this.unit = unit;
            this.paramProducer = paramProducer;
        }

        public String getType() {
            return type;
        }

        public String getRoutingKey() {
            return routingKey;
        }

        public long getInterval() {
            return interval;
        }
        
        public TimeUnit getUnit() {
            return unit;
        }
        
        private void prepareTaskParameters(Task task, RcObject instance) {
        	paramProducer.prepareTaskParameters(task, instance);
        }
        
    }
        
    private class PatrolParamProducer {
        private final List<PatrolParam>  params;
        private final String[] paramNames;
        
        private PatrolParamProducer(List<PatrolParam> params) {
            this.params = params;
            this.paramNames = params.stream().map(PatrolParam::getName).collect(Collectors.toList()).toArray(new String[0]);
        }
        
        private void prepareTaskParameters(Task task, RcObject instance) {
            task.param("instance", instance);
            RcValues values = instance.get(paramNames);
            for (PatrolParam param : params) {
                task.param(param.getName(), values.next().as(param.getType()));
            }
        }
    }

   private class PatrolParam {

        private final JavaType type;
        private final String name;

        private PatrolParam(String name, JavaType type) {
            super();
            this.name = name;
            this.type = type;
        }
        
        private JavaType getType() {
            return type;
        }
        
        private String getName() {
            return name;
        }
    }
}
