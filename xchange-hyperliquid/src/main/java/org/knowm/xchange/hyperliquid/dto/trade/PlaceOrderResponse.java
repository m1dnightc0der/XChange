package org.knowm.xchange.hyperliquid.dto.trade;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.math.BigDecimal;
import java.util.List;

/**
 * Response wrapper for place order requests
 * Matches the structure returned by Hyperliquid's /exchange endpoint for order placement
 *
 * Success response example:
 * {
 *   "status": "ok",
 *   "response": {
 *     "type": "order",
 *     "data": {
 *       "statuses": [
 *         {
 *           "filled": {
 *             "totalSz": "0.003",
 *             "avgPx": "4372.7",
 *             "oid": 192129729628
 *           }
 *         }
 *       ]
 *     }
 *   }
 * }
 *
 * Error response example:
 * {
 *   "status": "err",
 *   "response": "User or API Wallet 0x... does not exist."
 * }
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class PlaceOrderResponse {

    /**
     * Response status - "ok" for success, "err" for error
     */
    @JsonProperty("status")
    private String status;

    /**
     * Response data - can be OrderResponseData on success or String on error
     */
    @JsonProperty("response")
    private Object response;

    /**
     * Check if the order placement was successful
     * @return true if status is "ok"
     */
    public boolean isSuccess() {
        return "ok".equals(status);
    }

    /**
     * Check if the order placement failed
     * @return true if status is "err"
     */
    public boolean isError() {
        return "err".equals(status);
    }

    /**
     * Get error message if the request failed
     * @return Error message string, or null if not an error
     */
    public String getErrorMessage() {
        if (isError() && response instanceof String) {
            return (String) response;
        }
        return null;
    }

    /**
     * Get order response data if the request succeeded
     * @return OrderResponseData object, or null if error
     */
    @SuppressWarnings("unchecked")
    public OrderResponseData getOrderResponseData() {
        if (isSuccess() && response instanceof java.util.Map) {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            return mapper.convertValue(response, OrderResponseData.class);
        }
        return null;
    }

    /**
     * Inner class representing the order response data structure
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OrderResponseData {

        /**
         * Response type - typically "order"
         */
        @JsonProperty("type")
        private String type;

        /**
         * Order data containing statuses
         */
        @JsonProperty("data")
        private OrderData data;
    }

    /**
     * Inner class representing order data with statuses
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OrderData {

        /**
         * List of order statuses
         */
        @JsonProperty("statuses")
        private List<OrderStatus> statuses;
    }

    /**
     * Inner class representing individual order status
     * Can contain different status types: filled, resting, error, etc.
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OrderStatus {

        /**
         * Filled order information (when order is filled)
         */
        @JsonProperty("filled")
        private FilledInfo filled;

        /**
         * Resting order information (when order is placed but not filled)
         */
        @JsonProperty("resting")
        private RestingInfo resting;

        /**
         * Error information (when order placement fails)
         */
        @JsonProperty("error")
        private String error;

        /**
         * Check if this order was filled
         */
        public boolean isFilled() {
            return filled != null;
        }

        /**
         * Check if this order is resting (placed but not filled)
         */
        public boolean isResting() {
            return resting != null;
        }

        /**
         * Check if this order had an error
         */
        public boolean hasError() {
            return error != null;
        }

        /**
         * Get the order state
         * @return OrderState enum indicating the current state of the order
         */
        public OrderState getOrderState() {
            if (filled != null) {
                return OrderState.FILLED;
            } else if (resting != null) {
                return OrderState.RESTING;
            } else if (error != null) {
                return OrderState.ERROR;
            } else {
                return OrderState.UNKNOWN;
            }
        }

        /**
         * Get the order ID regardless of status type (filled or resting)
         * @return Order ID, or null if not available
         */
        public Long getOrderId() {
            if (filled != null && filled.getOrderId() != null) {
                return filled.getOrderId();
            }
            if (resting != null && resting.getOrderId() != null) {
                return resting.getOrderId();
            }
            return null;
        }
    }

    /**
     * Information about a filled order
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FilledInfo {

        /**
         * Total size filled as string
         */
        @JsonProperty("totalSz")
        private String totalSize;

        /**
         * Average fill price as string
         */
        @JsonProperty("avgPx")
        private String averagePrice;

        /**
         * Order ID
         */
        @JsonProperty("oid")
        private Long orderId;

        /**
         * Get total size as BigDecimal
         */
        public BigDecimal getTotalSizeAsBigDecimal() {
            try {
                return new BigDecimal(totalSize);
            } catch (NumberFormatException e) {
                return null;
            }
        }

        /**
         * Get average price as BigDecimal
         */
        public BigDecimal getAveragePriceAsBigDecimal() {
            try {
                return new BigDecimal(averagePrice);
            } catch (NumberFormatException e) {
                return null;
            }
        }
    }

    /**
     * Information about a resting (placed but not filled) order
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RestingInfo {

        /**
         * Order ID
         */
        @JsonProperty("oid")
        private Long orderId;
    }
}
