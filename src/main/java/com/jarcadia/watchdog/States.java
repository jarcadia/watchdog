package com.jarcadia.watchdog;

public class States {
	
	public enum InstanceState {
	    Enabled,
	    Draining,
	    Disabled,
	    Down,
	}
	
	public enum DistributionState {
	    PendingTransfer,
	    Transferring,
	    Transferred,

	    PendingCleanup,
	    CleaningUp,
	    CleanedUp
	}
}
