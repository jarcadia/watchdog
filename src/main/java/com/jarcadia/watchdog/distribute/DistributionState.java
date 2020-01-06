package com.jarcadia.watchdog.distribute;


public enum DistributionState {
    PendingTransfer,
    Transferring,
    Transferred,
    
    PendingVerification,
    Verifying,

    Verified
}
