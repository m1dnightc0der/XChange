package org.knowm.xchange.hyperliquid.dto.trade;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * Response wrapper for order status queries
 * Matches the structure returned by Hyperliquid's orderStatus API endpoint
 *
 * Example response:
 * {
 *   "status": "order",
 *   "order": {
 *     "order": { ... order details ... },
 *     "status": "open",
 *     "statusTimestamp": 1760005798693
 *   }
 * }
 */
@Data
public class OrderResponse {

    /**
     * Response status - typically "order" when order is found
     */
    @JsonProperty("status")
    private String status;

    /**
     * Order information wrapper containing the order details and status
     */
    @JsonProperty("order")
    private OrderInfo order;

    /**
     * Inner wrapper containing order details and current status
     */
    @Data
    public static class OrderInfo {

        /**
         * The actual order details
         */
        @JsonProperty("order")
        private Order order;

        /**
         * Current order status - "open", "filled", "canceled", "rejected", etc.
         */
        @JsonProperty("status")
        private String status;

        /**
         * Timestamp when the status was last updated (milliseconds since Unix epoch)
         */
        @JsonProperty("statusTimestamp")
        private long statusTimestamp;
    }

    /**
     * Convenience method to get the Order directly
     * @return The Order object, or null if not present
     */
    public Order getOrderDetails() {
        return order != null ? order.getOrder() : null;
    }

    /**
     * Convenience method to get the order status
     * @return The current order status string, or null if not present
     */
    public String getOrderStatus() {
        return order != null ? order.getStatus() : null;
    }

    /**
     * Convenience method to check if order was found
     * @return true if status is "order", false otherwise
     */
    public boolean isOrderFound() {
        return "order".equals(status);
    }
}
