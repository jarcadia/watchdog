package com.jarcadia.watchdog;

import java.util.List;

import com.jarcadia.rcommando.RedisObject;

public interface DeploymentAgent {

    /**
     * Implementations must eventually transition instance.state to Disabled
     */
    void disable(RedisObject instance);

    /**
     * Implementations must eventually transition instance.state to Down
     */
    void stop(RedisObject instance);

    /**
     * Implementations must eventually transition instance.deployState to Upgraded
     */
    void upgrade(RedisObject instance, RedisObject distribution);
    
    /**
     * Implementations must eventually transition instance.state to Disabled
     */
    void start(RedisObject instance);

    /**
     * Implementations must eventually transition instance.state to Enabled
     */
    void enable(RedisObject instance);

    /**
     * Implementations must eventually transition one of the provided instances to instance.deployState = Ready
     */
    void chooseNext(RedisObject deployment, List<RedisObject> instances);
}
