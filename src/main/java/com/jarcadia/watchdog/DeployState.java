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
    
    Complete,
    Failed;
}
