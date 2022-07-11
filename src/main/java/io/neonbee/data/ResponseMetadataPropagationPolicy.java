package io.neonbee.data;

public enum ResponseMetadataPropagationPolicy {
    /** response metadata will be merged and propagated up the chain automatically. */
    AUTO_MERGE,
    /**
     * response metadata will be not be merged and propagated up the chain automatically, it is the job of the verticle
     * to determine, whether and which metadata will be returned up the chain.
     */
    MANUAL_PROCESS
}
