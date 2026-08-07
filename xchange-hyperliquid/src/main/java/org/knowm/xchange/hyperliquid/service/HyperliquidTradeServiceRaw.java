package org.knowm.xchange.hyperliquid.service;

import org.knowm.xchange.exceptions.ExchangeException;
import org.knowm.xchange.exceptions.NotAvailableFromExchangeException;
import org.knowm.xchange.hyperliquid.HyperliquidExceptionAdapter;
import org.knowm.xchange.hyperliquid.HyperliquidExchange;
import org.knowm.xchange.hyperliquid.dto.Cloid;
import org.knowm.xchange.hyperliquid.dto.HyperliquidResponse;
import org.knowm.xchange.hyperliquid.dto.Response;
import org.knowm.xchange.hyperliquid.dto.account.HyperliquidClearinghouseState;
import org.knowm.xchange.hyperliquid.dto.trade.Fill;
import org.knowm.xchange.hyperliquid.dto.trade.Order;

import java.io.IOException;
import java.util.*;

/**
 * Raw service class for Hyperliquid authenticated API interactions
 */
public class HyperliquidTradeServiceRaw extends HyperliquidBaseService {

    // Shared metadata loader for coin name to asset index mapping
    private final HyperliquidMetadataLoader metadataLoader;

    public HyperliquidTradeServiceRaw(HyperliquidExchange exchange) {
        super(exchange);
        this.metadataLoader = new HyperliquidMetadataLoader(hyperliquidInfo);
    }

    /**
     * Get open orders for the authenticated user
     * Uses the public /info endpoint - no authentication required
     */
    public List<Order> getOpenOrdersRaw() throws IOException {
        if(exchange.getExchangeSpecification().getExchangeSpecificParameters().containsKey("wallet")) {
            Map<String, Object> requestBody = new LinkedHashMap<>();
            requestBody.put("type", "frontendOpenOrders");
            requestBody.put("user", (exchange.getExchangeSpecification().getExchangeSpecificParameters().get("vault")!=null ? exchange.getExchangeSpecification().getExchangeSpecificParameters().get("vault") : exchange.getExchangeSpecification().getExchangeSpecificParameters().get("wallet")));
            requestBody.put("dex", ""); // Default perp dex

            return hyperliquidInfo.getOpenOrders(
                    "application/json",
                    requestBody
            );
        } else {
            throw new ExchangeException("No wallet provied in exchangeSpecificParameters");
        }
    }

    /**
     * Query order status by order ID (OID)
     * Matches Python SDK: query_order_by_oid(user, oid)
     * Uses the public /info endpoint - no authentication required
     *
     * @param orderId The order ID to query
     * @return Order status information wrapped in OrderResponse
     * @throws IOException if the request fails
     */
    public org.knowm.xchange.hyperliquid.dto.trade.OrderResponse getOrderByOidRaw(Object orderId) throws IOException {
        if(exchange.getExchangeSpecification().getExchangeSpecificParameters().containsKey("wallet")) {
            Map<String, Object> requestBody = new LinkedHashMap<>();
            requestBody.put("type", "orderStatus");
            requestBody.put("user", (exchange.getExchangeSpecification().getExchangeSpecificParameters().get("vault")!=null ? exchange.getExchangeSpecification().getExchangeSpecificParameters().get("vault") : exchange.getExchangeSpecification().getExchangeSpecificParameters().get("wallet")));
            requestBody.put("oid", orderId);

            Object rawResponse = hyperliquidInfo.queryPublic(
                    "application/json",
                    requestBody
            );

            // Convert the raw response to OrderResponse using Jackson
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            return mapper.convertValue(rawResponse, org.knowm.xchange.hyperliquid.dto.trade.OrderResponse.class);
        } else {
            throw new ExchangeException("No wallet provided in exchangeSpecificParameters");
        }
    }

    /**
     * Retrieve user's fills (trade executions) within a specific time range
     * Matches Python SDK: user_fills_by_time(address, start_time, end_time, aggregate_by_time)
     * Uses the public /info endpoint - no authentication required
     *
     * @param startTime Unix timestamp in milliseconds (start of time range)
     * @param endTime Unix timestamp in milliseconds (end of time range, null for current time)
     * @param aggregateByTime When true, partial fills are combined when a crossing order gets filled
     *                        by multiple different resting orders. Resting orders filled by multiple
     *                        crossing orders will not be aggregated.
     * @return List of Fill objects containing trade execution details
     * @throws IOException if the request fails
     */
    public List<org.knowm.xchange.hyperliquid.dto.trade.Fill> getFillsByTime(long startTime, Long endTime, Boolean aggregateByTime) throws IOException {
        if(exchange.getExchangeSpecification().getExchangeSpecificParameters().containsKey("wallet")) {
            try {
                Map<String, Object> requestBody = new LinkedHashMap<>();
                requestBody.put("type", "userFillsByTime");
                requestBody.put("user", (exchange.getExchangeSpecification().getExchangeSpecificParameters().get("vault")!=null ? exchange.getExchangeSpecification().getExchangeSpecificParameters().get("vault") : exchange.getExchangeSpecification().getExchangeSpecificParameters().get("wallet")));
                requestBody.put("startTime", startTime);
                requestBody.put("endTime", endTime);
                requestBody.put("aggregateByTime", aggregateByTime);

                Object rawResponse = hyperliquidInfo.queryPublic(
                        "application/json",
                        requestBody
                );

                // Convert the raw response to List<Fill> using Jackson
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                return mapper.convertValue(
                        rawResponse,
                        mapper.getTypeFactory().constructCollectionType(List.class, org.knowm.xchange.hyperliquid.dto.trade.Fill.class)
                );
            }   catch (Exception e) {
                return new ArrayList<Fill>();
            }
        } else {
            throw new ExchangeException("No wallet provided in exchangeSpecificParameters");
        }
    }

    /**
     * Retrieve user's fills (trade executions) within a specific time range (simplified version)
     * Matches Python SDK: user_fills_by_time(address, start_time, end_time)
     *
     * @param startTime Unix timestamp in milliseconds (start of time range)
     * @param endTime Unix timestamp in milliseconds (end of time range, null for current time)
     * @return List of Fill objects containing trade execution details
     * @throws IOException if the request fails
     */
    public List<org.knowm.xchange.hyperliquid.dto.trade.Fill> getFillsByTime(long startTime, Long endTime) throws IOException {
        return getFillsByTime(startTime, endTime, false);
    }


    /**
     * Cancel a single order
     * Matches Python SDK: cancel(name, oid)
     *
     * @param name The coin/asset name (e.g., "BTC", "ETH")
     * @param orderId The order ID to cancel
     * @return Response from the exchange (result can be Map or String depending on success/error)
     * @throws IOException if the request fails
     */


    public HyperliquidResponse cancelByCloid(String name, String cloid) throws IOException {
        // Create cancel wire in Hyperliquid format with short field names
        // Matches Python SDK bulk_cancel structure (lines 277-283)
        Map<String, Object> cancelWire = new LinkedHashMap<>();
        cancelWire.put("asset", getAssetIndex(name));  // asset index
        cancelWire.put("cloid", cloid);              // order ID

        // Create action wrapper
        Map<String, Object> action = new LinkedHashMap<>();
        action.put("type", "cancelByCloid");
        action.put("cancels", java.util.Arrays.asList(cancelWire));

        Map<String, Object> requestBody =
                hyperliquidAuth.createSignedActionRequest(action, null);
        return sendCancel(requestBody);
    }
    public HyperliquidResponse cancel(String name, String orderId) throws IOException {
        // Create cancel wire in Hyperliquid format with short field names
        // Matches Python SDK bulk_cancel structure (lines 277-283)
        Map<String, Object> cancelWire = new LinkedHashMap<>();
        cancelWire.put("a", getAssetIndex(name));  // asset index
        cancelWire.put("o", Long.valueOf(orderId));              // order ID

        // Create action wrapper
        Map<String, Object> action = new LinkedHashMap<>();
        action.put("type", "cancel");
        action.put("cancels", java.util.Arrays.asList(cancelWire));

        Map<String, Object> requestBody =
                hyperliquidAuth.createSignedActionRequest(action, null);
        return sendCancel(requestBody);
    }
    /**
     * Get user account state
     * Uses the public /info endpoint - no authentication required
     */
    public Object getUserStateRaw() throws IOException {
        Map<String, Object> requestBody = new LinkedHashMap<>();
        requestBody.put("type", "clearinghouseState");
        requestBody.put("user", getUserAddress());
        requestBody.put("dex", ""); // Default perp dex

        return hyperliquidInfo.getUserState(
                "application/json",
                requestBody
        );
    }

    /**
     * Get clearinghouse state (perpetuals account summary) for the configured wallet.
     * Uses the public /info endpoint - no authentication required.
     *
     * @return HyperliquidClearinghouseState containing positions, margin summaries, etc.
     * @throws IOException if the request fails
     */
    public HyperliquidClearinghouseState getClearinghouseState() throws IOException {
        String walletAddress = (String) (exchange.getExchangeSpecification().getExchangeSpecificParameters().get("vault")!=null ? exchange.getExchangeSpecification().getExchangeSpecificParameters().get("vault") : exchange.getExchangeSpecification().getExchangeSpecificParameters().get("wallet"));

        if (walletAddress == null || walletAddress.isEmpty()) {
            throw new IllegalStateException("Wallet address not configured. " +
                    "Set 'wallet' in exchange specific parameters.");
        }

        return getClearinghouseState(walletAddress);
    }

    /**
     * Get clearinghouse state (perpetuals account summary) for a specific wallet address.
     * Uses the public /info endpoint - no authentication required.
     *
     * @param userAddress The user's wallet address in 42-character hexadecimal format
     * @return HyperliquidClearinghouseState containing positions, margin summaries, etc.
     * @throws IOException if the request fails
     */
    public HyperliquidClearinghouseState getClearinghouseState(String userAddress) throws IOException {
        Map<String, Object> requestBody = new LinkedHashMap<>();
        requestBody.put("type", "clearinghouseState");
        requestBody.put("user", userAddress);

        return hyperliquidInfo.getClearinghouseState("application/json", requestBody);
    }

    /**
     * Place a limit order
     * Uses the authenticated /exchange endpoint
     * Signature matches Python SDK: order(name, is_buy, sz, limit_px, order_type, reduce_only, cloid, builder)
     *
     * @return PlaceOrderResponse containing order placement result
     */
    public org.knowm.xchange.hyperliquid.dto.trade.PlaceOrderResponse placeOrderRaw(String name, boolean isBuy, double sz, double limitPx,
                                Map<String, Object> orderType, boolean reduceOnly,
                                Cloid cloid, Map<String, Object> builder) throws IOException {
        // Use HyperliquidAdapters for common order placement logic
        Map<String, Object> orderWire = org.knowm.xchange.hyperliquid.HyperliquidAdapters.createOrderWire(
                getAssetIndex(name),
                isBuy,
                limitPx,
                sz,
                reduceOnly,
                orderType,
                cloid
        );

        Map<String, Object> action = org.knowm.xchange.hyperliquid.HyperliquidAdapters.createOrderAction(
                java.util.Arrays.asList(orderWire),
                builder
        );

        Map<String, Object> requestBody =
                hyperliquidAuth.createSignedActionRequest(action, null);
        Object rawResponse = sendOrder(requestBody);

        // Convert raw response to typed PlaceOrderResponse
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        return mapper.convertValue(rawResponse, org.knowm.xchange.hyperliquid.dto.trade.PlaceOrderResponse.class);
    }

    /**
     * Place a limit order with simplified signature (backward compatibility)
     * Uses the authenticated /exchange endpoint
     *
     * @return PlaceOrderResponse containing order placement result
     */
    public org.knowm.xchange.hyperliquid.dto.trade.PlaceOrderResponse placeOrderRaw(String name, boolean isBuy, double sz, double limitPx,
                                String orderType, boolean reduceOnly) throws IOException {
        return placeOrderRaw(name, isBuy, sz, limitPx,
                org.knowm.xchange.hyperliquid.HyperliquidAdapters.createOrderType(orderType),
                reduceOnly, null, null);
    }

    /**
     * Modify an existing order
     * Matches Python SDK: modify_order(oid, name, is_buy, sz, limit_px, order_type, reduce_only, cloid)
     * Uses the authenticated /exchange endpoint
     *
     * @param orderId The order ID to modify (can be numeric order ID or client order ID)
     * @param name The coin/asset name (e.g., "BTC", "ETH")
     * @param isBuy True for buy, false for sell
     * @param sz New order size
     * @param limitPx New limit price
     * @param orderType Order type structure (e.g., {"limit": {"tif": "Gtc"}})
     * @param reduceOnly True for reduce-only orders
     * @param cloid Optional client order ID for the modified order
     * @return PlaceOrderResponse containing order modification result
     * @throws IOException if the request fails
     */
    public org.knowm.xchange.hyperliquid.dto.trade.PlaceOrderResponse modifyOrderRaw(
            Object orderId,
            String name,
            boolean isBuy,
            double sz,
            double limitPx,
            Map<String, Object> orderType,
            boolean reduceOnly,
            Cloid cloid) throws IOException {

        // Create order wire for the new order parameters
        Map<String, Object> orderWire = org.knowm.xchange.hyperliquid.HyperliquidAdapters.createOrderWire(
                getAssetIndex(name),
                isBuy,
                limitPx,
                sz,
                reduceOnly,
                orderType,
                cloid
        );

        // Create modify wire structure
        // Matches Python: {"oid": orderId, "order": orderWire}
       // Map<String, Object> modifyWire = new LinkedHashMap<>();
       // modifyWire.put("oid", orderId);
       // modifyWire.put("order", orderWire);

        // Create batchModify action
        // Matches Python SDK bulk_modify_orders_new (lines 183-196)
        Map<String, Object> action = new LinkedHashMap<>();
        action.put("type", "modify");
        action.put("oid", ((orderId instanceof Cloid) ?   ((Cloid) orderId).toRaw() : orderId));
        action.put("order", orderWire);
        //action.put(java.util.Arrays.asList(modifyWire));

        Map<String, Object> requestBody =
                hyperliquidAuth.createSignedActionRequest(action, null);
        Object rawResponse = sendOrder(requestBody);

        // Convert raw response to typed PlaceOrderResponse
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        return mapper.convertValue(rawResponse, org.knowm.xchange.hyperliquid.dto.trade.PlaceOrderResponse.class);
    }

    /**
     * Modify an existing order with simplified signature
     *
     * @param orderId The order ID to modify
     * @param name The coin/asset name
     * @param isBuy True for buy, false for sell
     * @param sz New order size
     * @param limitPx New limit price
     * @param orderType Order type string (e.g., "gtc", "ioc")
     * @param reduceOnly True for reduce-only orders
     * @return PlaceOrderResponse containing order modification result
     * @throws IOException if the request fails
     */
    public org.knowm.xchange.hyperliquid.dto.trade.PlaceOrderResponse modifyOrderRaw(
            Object orderId,
            String name,
            boolean isBuy,
            double sz,
            double limitPx,
            String orderType,
            boolean reduceOnly) throws IOException {
        return modifyOrderRaw(
                orderId,
                name,
                isBuy,
                sz,
                limitPx,
                org.knowm.xchange.hyperliquid.HyperliquidAdapters.createOrderType(orderType),
                reduceOnly,
                null
        );
    }

    private HyperliquidResponse sendCancel(Map<String, Object> requestBody) throws IOException {
        return sendMutation(
                () -> hyperliquidAuthenticated.cancelOrder("application/json", requestBody));
    }

    private Object sendOrder(Map<String, Object> requestBody) throws IOException {
        return sendMutation(
                () -> hyperliquidAuthenticated.placeOrder("application/json", requestBody));
    }

    @FunctionalInterface
    interface MutationRequest<T> {
        T send() throws IOException;
    }

    static <T> T sendMutation(MutationRequest<T> request) throws IOException {
        try {
            return request.send();
        } catch (IOException | RuntimeException failure) {
            org.knowm.xchange.exceptions.NonceException nonceFailure =
                    HyperliquidExceptionAdapter.nonceException(failure);
            if (nonceFailure != null) {
                throw nonceFailure;
            }
            throw failure;
        }
    }

    /**
     * Convert coin name to asset index
     * Delegates to shared metadata loader
     *
     * @param name The coin/asset name (e.g., "BTC", "ETH", "SOL")
     * @return The asset index for API requests
     * @throws IOException if metadata cannot be loaded
     */
    private int getAssetIndex(String name) throws IOException {
        return metadataLoader.getAssetIndex(name);
    }
    
    /**
     * Get user address from private key
     */
    private String getUserAddress() {
        if (hyperliquidAuth == null) {
            return null;
        }
        return hyperliquidAuth.getEthereumAddress();
    }
}