package com.jarcadia.watchdog.deploy;

public enum DeployState {
    
    
//    Distributing,
//    Distributed,
    Waiting,
    Ready,
    
    PendingDisable,
    Disabling,
//    Offline,

    PendingStop,
    Stopping,
//    Stopped,
    
    PendingUpgrade,
    PendingDistribution,
    Upgrading,
    Upgraded,
    
    PendingStart,
    Starting,
//    Started,

    PendingEnable,
    Enabling,

    Online,
    Idle,
    
    Failed;
    
    public boolean is(DeployState other) {
        return this == other;
    }
    
    public boolean isIn(DeployState... states) {
        for (DeployState state : states) {
            if (state == this) {
                return true;
            }
        }
        return false;
    }

}
