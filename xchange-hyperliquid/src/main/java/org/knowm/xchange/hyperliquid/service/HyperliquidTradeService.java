package org.knowm.xchange.hyperliquid.service;

import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.account.OpenPosition;
import org.knowm.xchange.dto.account.OpenPositions;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.dto.trade.MarketOrder;
import org.knowm.xchange.dto.trade.OpenOrders;
import org.knowm.xchange.dto.trade.UserTrades;
import org.knowm.xchange.exceptions.NotAvailableFromExchangeException;
import org.knowm.xchange.exceptions.NotYetImplementedForExchangeException;
import org.knowm.xchange.hyperliquid.HyperliquidAdapters;
import org.knowm.xchange.hyperliquid.HyperliquidExchange;
import org.knowm.xchange.hyperliquid.dto.HyperliquidResponse;
import org.knowm.xchange.hyperliquid.dto.account.HyperliquidClearinghouseState;
import org.knowm.xchange.hyperliquid.dto.trade.Fill;
import org.knowm.xchange.hyperliquid.dto.trade.HyperliquidTradeParams;
import org.knowm.xchange.hyperliquid.dto.trade.PlaceOrderResponse;
import org.knowm.xchange.service.trade.TradeService;
import org.knowm.xchange.service.trade.params.CancelOrderByIdParams;
import org.knowm.xchange.service.trade.params.CancelOrderByInstrument;
import org.knowm.xchange.service.trade.params.CancelOrderParams;
import org.knowm.xchange.service.trade.params.TradeHistoryParams;
import org.knowm.xchange.service.trade.params.orders.ClientOrderIdQueryParamInstrument;
import org.knowm.xchange.service.trade.params.orders.OpenOrdersParams;
import org.knowm.xchange.service.trade.params.orders.OrderQueryParamInstrument;
import org.knowm.xchange.service.trade.params.orders.OrderQueryParams;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Hyperliquid trade service implementation
 * Based on DeribitTradeService pattern
 */
public class HyperliquidTradeService extends HyperliquidTradeServiceRaw implements TradeService {

    public HyperliquidTradeService(HyperliquidExchange exchange) {
        super(exchange);
    }

    @Override
    public OpenOrders getOpenOrders() throws IOException {
        List<org.knowm.xchange.hyperliquid.dto.trade.Order> rawResponse = getOpenOrdersRaw();
        return HyperliquidAdapters.adaptOpenOrders(rawResponse);
        // TODO: Convert raw response to OpenOrders using HyperliquidAdapters
        // For now, return empty OpenOrders as placeholder

    }

    @Override
    public OpenOrders getOpenOrders(OpenOrdersParams params) throws IOException {
        // For now, ignore params and use basic implementation
        return getOpenOrders();
    }

    @Override
    public OpenOrdersParams createOpenOrdersParams() {
        throw new NotYetImplementedForExchangeException("createOpenOrdersParams not yet implemented");
    }

    @Override
    public String placeMarketOrder(MarketOrder marketOrder) throws IOException {
        throw new NotYetImplementedForExchangeException("placeMarketOrder not yet implemented");
    }

    @Override
    public String placeLimitOrder(LimitOrder limitOrder) throws IOException {
        // Extract parameters from XChange LimitOrder
        String coin = limitOrder.getInstrument().getBase().getCurrencyCode();
        boolean isBuy = limitOrder.getType().equals(Order.OrderType.BID);
        double size = limitOrder.getOriginalAmount().doubleValue();
        double limitPrice = limitOrder.getLimitPrice().doubleValue();
        String userRef = limitOrder.getUserReference();
        boolean reduceOnly = false; // Default to false

        // Create order type structure (default to GTC limit order)
        java.util.Map<String, Object> limitType = new java.util.LinkedHashMap<>();
        limitType.put("tif", "Gtc");
        java.util.Map<String, Object> orderType = new java.util.LinkedHashMap<>();
        orderType.put("limit", limitType);

        // Place the order using raw API (calls the new signature with proper signing)
        PlaceOrderResponse rawResponse = placeOrderRaw(coin, isBuy, size, limitPrice, orderType, reduceOnly, HyperliquidAdapters.adaptUserReferenceToCoid(userRef), null);

        // TODO: Parse response and extract order ID
        // For now, return a placeholder - in a full implementation we would parse the JSON response
        // and extract the order ID or handle errors appropriately

        // Check if response indicates success and extract order ID
        if (rawResponse != null && rawResponse.isSuccess()) {
            PlaceOrderResponse.OrderResponseData orderData = rawResponse.getOrderResponseData();
            if (orderData != null && orderData.getData() != null &&
                    orderData.getData().getStatuses() != null &&
                    !orderData.getData().getStatuses().isEmpty()) {

                PlaceOrderResponse.OrderStatus status = orderData.getData().getStatuses().get(0);
                Long orderId = status.getOrderId();

                if (orderId != null) {
                    return orderId.toString();
                } else if (status.hasError()) {
                    throw new IOException("Order placement failed: " + status.getError());
                } else {
                    throw new IOException("Order placement failed: no order ID returned");
                }
            } else {
                throw new IOException("Order placement failed: invalid response structure");
            }
        } else if (rawResponse != null && rawResponse.isError()) {
            throw new IOException("Order placement failed: " + rawResponse.getErrorMessage());
        } else {
            throw new IOException("Failed to place limit order - no response received");
        }
    }

    @Override
    public String changeOrder(LimitOrder limitOrder) throws IOException {
        // Extract order ID - must be present to modify an order
        String orderId = limitOrder.getId();
        if (orderId == null || orderId.isEmpty()) {
            throw new IOException("Order ID is required to modify an order");
        }

        // Extract parameters from XChange LimitOrder
        String coin = limitOrder.getInstrument().getBase().getCurrencyCode();
        boolean isBuy = limitOrder.getType().equals(Order.OrderType.BID);
        double size = limitOrder.getOriginalAmount().doubleValue();
        double limitPrice = limitOrder.getLimitPrice().doubleValue();
        String userRef = limitOrder.getUserReference();
        boolean reduceOnly = false; // Default to false

        // Create order type structure (default to GTC limit order)
        java.util.Map<String, Object> limitType = new java.util.LinkedHashMap<>();
        limitType.put("tif", "Gtc");
        java.util.Map<String, Object> orderType = new java.util.LinkedHashMap<>();
        orderType.put("limit", limitType);

        // Determine if orderId is a numeric order ID or a client order ID
        Object oid;
        try {
            // Try to parse as Long (numeric order ID)
            oid = Long.parseLong(orderId);
        } catch (NumberFormatException e) {
            // If not numeric, treat as client order ID (hexadecimal string)
            oid = HyperliquidAdapters.adaptUserReferenceToCoid(orderId);

        }

        // Modify the order using raw API
        PlaceOrderResponse rawResponse = modifyOrderRaw(
                oid,
                coin,
                isBuy,
                size,
                limitPrice,
                orderType,
                reduceOnly,
                HyperliquidAdapters.adaptUserReferenceToCoid(userRef)
        );


        // Check if response indicates success and extract order ID
        if (rawResponse != null && rawResponse.isSuccess()) {
            PlaceOrderResponse.OrderResponseData orderData = rawResponse.getOrderResponseData();
            if (orderData != null && orderData.getData() != null &&
                    orderData.getData().getStatuses() != null &&
                    !orderData.getData().getStatuses().isEmpty()) {

                PlaceOrderResponse.OrderStatus status = orderData.getData().getStatuses().get(0);
                Long newOrderId = status.getOrderId();

                if (newOrderId != null) {
                    return newOrderId.toString();
                } else if (status.hasError()) {
                    throw new IOException("Order modification failed: " + status.getError());
                } else {
                    throw new IOException("Order modification failed: no order ID returned");
                }
            } else if (orderData.getType() != null) {
                return null;
            } else {
                throw new IOException("Order modification failed: invalid response structure");
            }
        } else if (rawResponse != null && rawResponse.isError()) {
            throw new IOException("Order modification failed: " + rawResponse.getErrorMessage());
        } else {
            throw new IOException("Failed to modify order - no response received");
        }
    }

    @Override
    public boolean cancelOrder(String orderId) throws IOException {

        throw new NotAvailableFromExchangeException("cancel by order id only not support by hyperliquid. symbol needs to be provdied");

    }

    private boolean cancelOrder(String orderId, String symbol) throws IOException {

        HyperliquidResponse response;
        if (orderId.matches("\\d+")) {
            response = super.cancel(symbol, orderId);
        } else {
            response = super.cancelByCloid(symbol, orderId);
        }
        if (response != null && response.getStatus().equals("err")) {
            throw new IOException((response.getResult() != null ? response.getResult().toString() : "Failed to cancel order ID " + orderId));
        }
        // Result can be a Map (success) or String (error message)
        // For success, the structure is: {"type": "cancel", "data": {"statuses": ["success"]}}
        Map result = (Map) response.getResult();

        // Optionally validate the response structure
        if (result instanceof java.util.Map) {
            @SuppressWarnings("unchecked")
            java.util.Map<String, Object> resultMap = (java.util.Map<String, Object>) result;
            String type = (String) resultMap.get("type");
            if (!"cancel".equals(type)) {
                throw new IOException("Unexpected response type: " + type);
            }
        }

        return true;
    }

    @Override
    public boolean cancelOrder(CancelOrderParams params) throws IOException {

        if (params instanceof HyperliquidTradeParams.HyperliquidCancelOrderParams) {
            HyperliquidTradeParams.HyperliquidCancelOrderParams hyperliquidCancelOrderParams = (HyperliquidTradeParams.HyperliquidCancelOrderParams) params;
            String id = ((CancelOrderByIdParams) params).getOrderId();
            String instrumentId = hyperliquidCancelOrderParams.getInstrument().getBase().toString();
            return cancelOrder(id, instrumentId);
        } else if (params instanceof CancelOrderByIdParams && params instanceof CancelOrderByInstrument) {

            String id = ((CancelOrderByIdParams) params).getOrderId();
            String instrumentId = ((CancelOrderByInstrument) params).getInstrument().getBase().toString();
            return cancelOrder(id, instrumentId);


        } else {
            throw new IOException("CancelOrderParams must implement CancelOrderByIdParams and CancelOrderByInstrument interface.");
        }


    }

    @Override
    public UserTrades getTradeHistory(TradeHistoryParams params) throws IOException {
        throw new NotYetImplementedForExchangeException("getTradeHistory not yet implemented");
    }

    @Override
    public TradeHistoryParams createTradeHistoryParams() {
        throw new NotYetImplementedForExchangeException("createTradeHistoryParams not yet implemented");
    }

    @Override
    public OpenPositions getOpenPositions() throws IOException {
        HyperliquidClearinghouseState state = getClearinghouseState();
        Collection<OpenPosition> positions = HyperliquidAdapters.adaptOpenPositions(state);
        return new OpenPositions(new ArrayList<>(positions));
    }

    @Override
    public Collection<Order> getOrder(OrderQueryParams... orderQueryParams) throws IOException {
        String[] orderIds = Arrays.stream(orderQueryParams)
                .filter(orderQueryParam -> orderQueryParam instanceof OrderQueryParamInstrument)
                .map(OrderQueryParams::getOrderId)
                .toArray(String[]::new);


        String[] clientOrderIds = Arrays.stream(orderQueryParams)
                .filter(orderQueryParam -> orderQueryParam instanceof ClientOrderIdQueryParamInstrument)
                .map(OrderQueryParams::getOrderId)
                .toArray(String[]::new);


        ArrayList<Order> orders = new ArrayList<>();


        for (String orderId : orderIds) {
            org.knowm.xchange.hyperliquid.dto.trade.OrderResponse response = getOrderByOidRaw(Long.valueOf(orderId));

            // Check if order was found and extract the order details
            if (response.isOrderFound() && response.getOrderDetails() != null) {
                org.knowm.xchange.hyperliquid.dto.trade.Order hyperliquidOrder = response.getOrderDetails();

                // Convert Hyperliquid order to XChange Order
                Order xchangeOrder = HyperliquidAdapters.adaptOrder(hyperliquidOrder);
                Order.OrderStatus orderStatus = HyperliquidAdapters.adaptOrderStatus(response.getOrderStatus());
                xchangeOrder.setOrderStatus(orderStatus);
                if (xchangeOrder.getCumulativeAmount().compareTo(BigDecimal.ZERO) != 0) {
                    List<Fill> fills = getFillsByTime(xchangeOrder.getTimestamp().getTime(), (new Date()).getTime()).stream()
                            .filter(fill -> xchangeOrder.getId().equals(fill.getOrderId().toString()))
                            .collect(Collectors.toList());
                    ;
                    if (!fills.isEmpty()) {
                        // Calculate weighted average price
                        BigDecimal totalValue = fills.stream()
                                .map(fill -> fill.getPriceAsBigDecimal().multiply(fill.getSizeAsBigDecimal()))
                                .reduce(BigDecimal.ZERO, BigDecimal::add);

                        BigDecimal totalSize = fills.stream()
                                .map(Fill::getSizeAsBigDecimal)
                                .reduce(BigDecimal.ZERO, BigDecimal::add);

                        BigDecimal avgPrice = totalSize.compareTo(BigDecimal.ZERO) > 0
                                ? totalValue.divide(totalSize, 8, BigDecimal.ROUND_HALF_UP)
                                : BigDecimal.ZERO;

                        if (avgPrice.compareTo(BigDecimal.ZERO) != 0) {
                            xchangeOrder.setAveragePrice(avgPrice);
                            xchangeOrder.setCumulativeAmount(totalSize);
                        }
                    } else {
                        xchangeOrder.setCumulativeAmount(BigDecimal.ZERO);
                    }


                }
                orders.add(xchangeOrder);
            }


        }

        for (String orderId : clientOrderIds) {
            org.knowm.xchange.hyperliquid.dto.trade.OrderResponse response = getOrderByOidRaw(HyperliquidAdapters.adaptUserReferenceToCoid(orderId).toRaw());

            // Check if order was found and extract the order details
            if (response.isOrderFound() && response.getOrderDetails() != null) {
                org.knowm.xchange.hyperliquid.dto.trade.Order hyperliquidOrder = response.getOrderDetails();

                // Convert Hyperliquid order to XChange Order
                Order xchangeOrder = HyperliquidAdapters.adaptOrder(hyperliquidOrder);
                Order.OrderStatus orderStatus = HyperliquidAdapters.adaptOrderStatus(response.getOrderStatus());
                xchangeOrder.setOrderStatus(orderStatus);
                if (xchangeOrder.getCumulativeAmount().compareTo(BigDecimal.ZERO) != 0) {
                    List<Fill> fills = getFillsByTime(xchangeOrder.getTimestamp().getTime(), (new Date()).getTime()).stream()
                            .filter(fill -> xchangeOrder.getId().equals(fill.getOrderId().toString()))
                            .collect(Collectors.toList());
                    ;
                    if (!fills.isEmpty()) {
                        // Calculate weighted average price
                        BigDecimal totalValue = fills.stream()
                                .map(fill -> fill.getPriceAsBigDecimal().multiply(fill.getSizeAsBigDecimal()))
                                .reduce(BigDecimal.ZERO, BigDecimal::add);

                        BigDecimal totalSize = fills.stream()
                                .map(Fill::getSizeAsBigDecimal)
                                .reduce(BigDecimal.ZERO, BigDecimal::add);

                        BigDecimal avgPrice = totalSize.compareTo(BigDecimal.ZERO) > 0
                                ? totalValue.divide(totalSize, 8, BigDecimal.ROUND_HALF_UP)
                                : BigDecimal.ZERO;

                        if (avgPrice.compareTo(BigDecimal.ZERO) != 0) {
                            xchangeOrder.setAveragePrice(avgPrice);
                            xchangeOrder.setCumulativeAmount(totalSize);
                        }
                    } else {
                        xchangeOrder.setCumulativeAmount(BigDecimal.ZERO);
                    }


                }
                orders.add(xchangeOrder);
            }


        }
        return orders;
    }


    @Override
    public Class[] getRequiredCancelOrderParamClasses() {
        throw new NotYetImplementedForExchangeException("getRequiredCancelOrderParamClasses not yet implemented");
    }
}