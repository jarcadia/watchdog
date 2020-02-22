package com.jarcadia.watchdog.exception;

public class DiscoveryException extends RuntimeException {

    public DiscoveryException(String message)
    {
        super(message);
    }

    public DiscoveryException(Throwable cause)
    {
        super(cause);
    }

    public DiscoveryException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
