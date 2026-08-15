package info.bitrich.xchangestream.hyperliquid;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.core.StreamingTradeService;
import info.bitrich.xchangestream.hyperliquid.dto.HyperliquidPostMessage;
import info.bitrich.xchangestream.service.netty.StreamingObjectMapperHelper;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.dto.trade.UserTrade;
import org.knowm.xchange.exceptions.ExchangeException;
import org.knowm.xchange.hyperliquid.HyperliquidAdapters;
import org.knowm.xchange.hyperliquid.HyperliquidAuthenticated;
import org.knowm.xchange.hyperliquid.dto.Cloid;
import org.knowm.xchange.hyperliquid.dto.trade.TimeInForce;
import org.knowm.xchange.hyperliquid.service.HyperliquidAuth;
import org.knowm.xchange.instrument.Instrument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import si.mazi.rescu.SynchronizedValueFactory;
import org.knowm.xchange.utils.nonce.AtomicLongIncrementalTime2014NonceFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Hyperliquid WebSocket-based trade service
 *
 * Implements order placement and order update streaming via WebSocket POST requests
 * following the Hyperliquid API specification:
 * https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/post-requests
 */
public class HyperliquidStreamingTradeService implements StreamingTradeService {

    private static final Logger LOG = LoggerFactory.getLogger(HyperliquidStreamingTradeService.class);

    private final HyperliquidStreamingService service;
    private final HyperliquidAuth hyperliquidAuth;
    private final int responseTimeout;
    private final ObjectMapper mapper = StreamingObjectMapperHelper.getObjectMapper();
    private final SynchronizedValueFactory<Long> requestIdFactory =
            new AtomicLongIncrementalTime2014NonceFactory();

    // Shared metadata loader for coin name to asset index mapping
    private final org.knowm.xchange.hyperliquid.service.HyperliquidMetadataLoader metadataLoader;

    // Channel name for user order updates
    private static final String USER_EVENTS = "userEvents";
    private static final String USER_ORDERS = "orderUpdates";
    // Channel name for user fills
    private static final String USER_FILLS = "userFills";

    public HyperliquidStreamingTradeService(
            HyperliquidStreamingService service,
            HyperliquidAuth hyperliquidAuth,
            org.knowm.xchange.hyperliquid.service.HyperliquidMetadataLoader metadataLoader) {
        this(service, hyperliquidAuth, metadataLoader, 5000);
    }

    HyperliquidStreamingTradeService(
            HyperliquidStreamingService service,
            HyperliquidAuth hyperliquidAuth,
            org.knowm.xchange.hyperliquid.service.HyperliquidMetadataLoader metadataLoader,
            int responseTimeout) {
        this.service = service;
        this.hyperliquidAuth = hyperliquidAuth;
        this.metadataLoader = metadataLoader;
        this.responseTimeout = responseTimeout;
    }

    /**
     * Place a limit order via WebSocket POST request
     *
     * @param limitOrder The limit order to place
     * @return The order ID assigned by the exchange
     * @throws Exception if the order placement fails
     */
    @Override
    public String placeLimitOrder(LimitOrder limitOrder) throws Exception {
        // Convert XChange instrument to Hyperliquid coin symbol
        String coin = HyperliquidAdapters.adaptInstrumentToCoin(limitOrder.getInstrument());

        // Determine if buy or sell
        boolean isBuy = limitOrder.getType() == Order.OrderType.BID ||
                       limitOrder.getType() == Order.OrderType.EXIT_ASK;

        // Get order parameters
        double size = limitOrder.getOriginalAmount().doubleValue();
        double limitPx = limitOrder.getLimitPrice().doubleValue();
        boolean reduceOnly = false; // TODO: Check for reduce-only flag when supported

        // Convert user reference to Cloid if provided
        Cloid cloid = null;
        String userRef = limitOrder.getUserReference();

        Map<String, Object> orderType;
        // Use HyperliquidAdapters for common order placement logic
        if (limitOrder.getOrderFlags().contains(org.knowm.xchange.hyperliquid.dto.trade.TimeInForce.Alo)) {
            orderType = HyperliquidAdapters.createOrderType("alo");
        } else if (limitOrder.getOrderFlags().contains(TimeInForce.Ioc)) {
             orderType = HyperliquidAdapters.createOrderType("ioc");
        } else {
             orderType = HyperliquidAdapters.createOrderType("gtc");
        }


        Map<String, Object> orderWire = HyperliquidAdapters.createOrderWire(
                getAssetIndex(coin),
                isBuy,
                limitPx,
                size,
                reduceOnly,
                orderType,
                HyperliquidAdapters.adaptUserReferenceToCoid(userRef)
        );

        Map<String, Object> action = HyperliquidAdapters.createOrderAction(
                Arrays.asList(orderWire),
                null // no builder
        );

        Map<String, Object> payload = hyperliquidAuth.createSignedActionRequest(action, null);

        // WebSocket correlation IDs are independent from signed-action nonces.
        Long id = requestIdFactory.createValue();
        HyperliquidPostMessage.Request request =
                new HyperliquidPostMessage.Request("action", payload);
        HyperliquidPostMessage message =
                new HyperliquidPostMessage("post", id, request);

        LOG.debug("Placing limit order via WebSocket: {} {} {} @ {}",
                isBuy ? "BUY" : "SELL", size, coin, limitPx);

        // Send via WebSocket and wait for response
        @NonNull JsonNode response = service.subscribeSingle(
                id,
                mapper.writeValueAsString(message))
            .timeout(responseTimeout, MILLISECONDS)
            .blockingSingle();

        String explicitError = explicitMutationError(response);
        if (explicitError != null) {
            ExchangeException failure = orderPlacementFailure(explicitError);
            if (failure.getClass() != ExchangeException.class) {
                limitOrder.setOrderStatus(Order.OrderStatus.REJECTED);
            }
            throw failure;
        }

        JsonNode data = response.path("data").path("response").path("payload")
                .path("response").path("data");
        JsonNode statuses = data.path("statuses");
        if (statuses.isArray() && !statuses.isEmpty()) {
            JsonNode status = statuses.get(0);

            if (status.path("resting").hasNonNull("oid")) {
                String orderId = status.path("resting").get("oid").asText();
                LOG.info("Order placed successfully: {}", orderId);
                return orderId;
            }

            if (status.path("filled").hasNonNull("oid")) {
                String orderId = status.path("filled").get("oid").asText();
                LOG.info("Order immediately filled: {}", orderId);
                return orderId;
            }
        }

        LOG.error("Unexpected response format: {}", response);
        throw new IOException("Unexpected response format from exchange");
    }

    private String explicitMutationError(JsonNode response) {
        JsonNode responsePayload = response.path("data").path("response").path("payload");
        if ("err".equals(responsePayload.path("status").asText())) {
            return explicitText(responsePayload.get("response"));
        }

        JsonNode statuses = responsePayload.path("response").path("data").path("statuses");
        if (statuses.isArray() && !statuses.isEmpty()) {
            String statusError = explicitText(statuses.get(0).get("error"));
            if (statusError != null) {
                return statusError;
            }
        }

        String directError = explicitText(response.path("response").get("error"));
        return directError != null ? directError : explicitText(response.get("error"));
    }

    private String explicitText(JsonNode node) {
        if (node == null || !node.isTextual() || node.asText().trim().isEmpty()) {
            return null;
        }
        return node.asText();
    }

    private ExchangeException orderPlacementFailure(String error) {
        return HyperliquidAdapters.adaptError("Order placement failed: ", error);
    }

    /**
     * Subscribe to order change events for a specific instrument
     *
     * @param instrument The instrument to monitor (null for all instruments)
     * @param args Optional arguments
     * @return Observable stream of order updates
     */
    @Override
    public Observable<Order> getOrderChanges(Instrument instrument, Object... args) {
        String channelId = USER_ORDERS + "." + hyperliquidAuth.getWalletAddress().toLowerCase();

        return service
            .subscribeChannel(channelId)
            .doOnError(error -> {
                LOG.error("Error in order changes subscription", error);
            })
            .filter(message -> message.has("channel") &&
                   message.get("channel").asText().equals("orderUpdates"))
            .filter(message -> message.has("data"))
            .flatMap(jsonNode -> {
                JsonNode data = jsonNode.get("data");

                // Handle order updates
                if (data.has("fills") && data.get("fills").isArray()) {
                    // Parse fill updates (implies order state change)
                    Iterable<Order> fillOrders = parseFillUpdates(data.get("fills"), instrument);
                    return Observable.fromIterable(fillOrders);
                }

                if (data.isArray()) {
                    // Parse order updates

                        Iterable<Order> orderUpdates = parseOrderUpdates(data, instrument);
                        return Observable.fromIterable(orderUpdates);

                }

                return Observable.empty();
            });
    }

    /**
     * Parse fill updates from WebSocket message
     */
    private Iterable<Order> parseFillUpdates(JsonNode fills, Instrument filterInstrument) {
        java.util.List<Order> orders = new java.util.ArrayList<>();

        for (JsonNode fill : fills) {
            try {
                // Convert to Hyperliquid Order DTO
                org.knowm.xchange.hyperliquid.dto.trade.Order hyperliquidOrder =
                    mapper.treeToValue(fill,
                        org.knowm.xchange.hyperliquid.dto.trade.Order.class);

                // Convert to XChange Order
                Order order = HyperliquidAdapters.adaptOrder(hyperliquidOrder);

                // Filter by instrument if specified
                if (filterInstrument == null ||
                        order.getInstrument().equals(filterInstrument)) {
                    orders.add(order);
                }
            } catch (Exception e) {
                LOG.error("Error parsing fill update", e);
            }
        }

        return orders;
    }

    /**
     * Parse order updates from WebSocket message
     */
    private Iterable<Order> parseOrderUpdates(JsonNode orders, Instrument filterInstrument) {
        java.util.List<Order> orderList = new java.util.ArrayList<>();

        for (JsonNode orderNode : orders) {
            if (orderNode.has("order")) {

                try {
                    // Convert to Hyperliquid Order DTO
                    org.knowm.xchange.hyperliquid.dto.trade.Order hyperliquidOrder =
                            mapper.treeToValue(orderNode.get("order"),
                                    org.knowm.xchange.hyperliquid.dto.trade.Order.class);

                    // Convert to XChange Order using the status update timestamp when present.
                    Date statusTimestamp = orderNode.hasNonNull("statusTimestamp")
                            ? new Date(orderNode.get("statusTimestamp").asLong())
                            : null;
                    Order order = HyperliquidAdapters.adaptOrder(hyperliquidOrder, statusTimestamp);
                    if (orderNode.has("status")) {
                        Order.OrderStatus status = HyperliquidAdapters.adaptOrderStatus(orderNode.get("status").asText());
                        order.setOrderStatus(status);
                    }
                    // Filter by instrument if specified
                    if (filterInstrument == null ||
                            order.getInstrument().equals(filterInstrument)) {
                        orderList.add(order);
                    }
                } catch (Exception e) {
                    LOG.error("Error parsing order update", e);
                }

            }
        }

        return orderList;
    }

    /**
     * Subscribe to user fill (trade execution) events
     * Subscribes to the userFills WebSocket channel as per:
     * https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
     *
     * @param instrument The instrument to monitor (used for filtering, null for all instruments)
     * @param args Optional arguments
     * @return Observable stream of UserTrade updates
     */
    @Override
    public Observable<UserTrade> getUserTrades(Instrument instrument, Object... args) {
        String channelId = USER_FILLS + "." + hyperliquidAuth.getWalletAddress().toLowerCase();

        return service
            .subscribeChannel(channelId)
            .doOnError(error -> {
                LOG.error("Error in user fills subscription", error);
            })
            .filter(message -> message.has("channel") &&
                   message.get("channel").asText().equals("userFills"))
            .filter(message -> message.has("data"))
            .flatMap(jsonNode -> {
                JsonNode data = jsonNode.get("data");

                // Parse fills array
                if (data.has("fills") && data.get("fills").isArray()) {
                    java.util.List<UserTrade> userTrades = parseFills(data.get("fills"), instrument);
                    return Observable.fromIterable(userTrades);
                }

                return Observable.empty();
            });
    }

    /**
     * Parse fills from WebSocket message into UserTrades
     */
    private java.util.List<UserTrade> parseFills(JsonNode fills, Instrument filterInstrument) {
        java.util.List<UserTrade> userTrades = new java.util.ArrayList<>();

        for (JsonNode fillNode : fills) {
            try {
                // Convert to Hyperliquid Fill DTO
                org.knowm.xchange.hyperliquid.dto.trade.Fill hyperliquidFill =
                    mapper.treeToValue(fillNode,
                        org.knowm.xchange.hyperliquid.dto.trade.Fill.class);

                // Convert to XChange UserTrade
                UserTrade userTrade = HyperliquidAdapters.adaptUserTrade(hyperliquidFill);

                // Filter by instrument if specified
                if (filterInstrument == null ||
                        userTrade.getInstrument().equals(filterInstrument)) {
                    userTrades.add(userTrade);
                }
            } catch (Exception e) {
                LOG.error("Error parsing fill", e);
            }
        }

        return userTrades;
    }

    /**
     * Get asset index for a coin name
     * Delegates to shared metadata loader
     */
    private int getAssetIndex(String name) throws java.io.IOException {
        return metadataLoader.getAssetIndex(name);
    }

}
