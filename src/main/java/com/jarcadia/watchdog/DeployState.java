package com.jarcadia.watchdog;

public enum DeployState {
    Waiting,
    Ready,

    PendingDrain,
    Draining,

    PendingStop,
    Stopping,

    PendingUpgrade,
    PendingDistribution,
    Upgrading,
    Upgraded,

    PendingStart,
    Starting,

    PendingEnable,
    Enabling,
    
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
