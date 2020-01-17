//package com.jarcadia.watchdog;
//
//import java.util.List;
//
//import com.jarcadia.rcommando.RedisObject;
//
//public interface DeploymentAgent {
//    
//    /**
//     * Implementations must eventually transition transfer.state = DistributionState.Transferred 
//     */
////    void transfer(String host, RedisObject transfer, RedisObject artifact);
//    
//    /**
//     * Implementations must eventually transition transfer.state = DistributionState.CleanedUp 
//     */
////    void cleanup(String host, RedisObject transfer, RedisObject artifact);
//
//    /**
//     * Implementations must eventually transition instance.state to InstanceState.Disabled
//     */
////    void disable(RedisObject instance);
//
//    /**
//     * Implementations must eventually transition instance.state to InstanceState.Down
//     */
////    void stop(RedisObject instance);
//
//    /**
//     * Implementations must eventually transition instance.deployState to DeployState.Upgraded
//     */
////    void upgrade(RedisObject instance, RedisObject artifact, RedisObject transfer);
//    
//    /**
//     * Implementations must eventually transition instance.state to InstanceState.Disabled
//     */
////    void start(RedisObject instance);
//
//    /**
//     * Implementations must eventually transition instance.state to InstanceState.Enabled
//     */
////    void enable(RedisObject instance);
//
//    /**
//     * Implementations must eventually transition one of the provided instances to instance.deployState = DeployState.Ready
//     */
////    void chooseNext(RedisObject deployment, List<RedisObject> instances);
//}
