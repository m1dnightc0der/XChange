package org.knowm.xchange.hyperliquid.dto.trade;

/**
 * Enum representing the state of an order in Hyperliquid
 * Used in order placement responses to indicate whether the order was filled or is resting
 */
public enum OrderState {
    /**
     * Order was immediately filled (fully or partially)
     */
    FILLED,

    /**
     * Order is resting on the order book (not yet filled)
     */
    RESTING,

    /**
     * Order encountered an error during placement
     */
    ERROR,

    /**
     * Order state is unknown or not specified
     */
    UNKNOWN
}
