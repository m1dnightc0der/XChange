package org.knowm.xchange.hyperliquid;

import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.derivative.FuturesContract;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.marketdata.*;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.dto.trade.OpenOrders;
import org.knowm.xchange.hyperliquid.dto.Cloid;
import org.knowm.xchange.hyperliquid.dto.marketdata.HyperliquidAllMids;
import org.knowm.xchange.hyperliquid.dto.marketdata.HyperliquidCandleSnapshot;
import org.knowm.xchange.hyperliquid.dto.marketdata.HyperliquidL2Book;
import org.knowm.xchange.hyperliquid.dto.marketdata.HyperliquidTrade;
import org.knowm.xchange.hyperliquid.dto.trade.Side;
import org.knowm.xchange.instrument.Instrument;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Utility class to adapt between Hyperliquid DTOs and XChange DTOs
 */
public class HyperliquidAdapters {

    /**
     * Adapt instrument to coin symbol for Hyperliquid API
     */
    public static String adaptInstrumentToCoin(Instrument instrument) {
        if (instrument instanceof FuturesContract) {
            FuturesContract pair = (FuturesContract) instrument;
            return pair.getCurrencyPair().getBase().getCurrencyCode();
        } else {

            return instrument.getBase().getCurrencyCode() + "-" + instrument.getCounter().getCurrencyCode();
        }

    }

    /**
     * Adapt Hyperliquid allMids response to XChange Ticker
     */
    public static Ticker adaptTicker(HyperliquidAllMids allMids, Instrument instrument) {
        String coin = adaptInstrumentToCoin(instrument);
        BigDecimal midPrice = allMids.getValue(coin);

        if (midPrice == null) {
            return null;
        }

        return new Ticker.Builder()
                .instrument(instrument)
                .last(midPrice)
                .timestamp(new Date())
                .build();
    }


    private static List<LimitOrder> adaptOrdersList(
            TreeMap<BigDecimal, BigDecimal> map, Order.OrderType type, Instrument instrument, Date timestamp) {
        return map.entrySet().stream()
                .map(e -> new LimitOrder(type, e.getValue(), instrument, null, timestamp, e.getKey()))
                .collect(Collectors.toList());
    }

    /**
     * Adapt Hyperliquid L2Book response to XChange OrderBook
     */
    public static OrderBook adaptOrderBook(HyperliquidL2Book l2Book, Instrument instrument) {
        List<LimitOrder> bids =
                adaptOrdersList(l2Book.getBids(), Order.OrderType.BID, instrument, l2Book.getTimestamp());
        List<LimitOrder> asks =
                adaptOrdersList(l2Book.getAsks(), Order.OrderType.ASK, instrument, l2Book.getTimestamp());
        return new OrderBook(l2Book.getTimestamp(), asks, bids);
    }

    public static Trades adaptTrades(List<HyperliquidTrade> hyperliquidTrades, Instrument instrument) {

        return new Trades(
                hyperliquidTrades.stream()
                        .map(trade -> adaptTrade(trade, instrument))
                        .collect(Collectors.toList()));
    }

    public static Trade adaptTrade(HyperliquidTrade trade, Instrument instrument) {
        return new Trade.Builder()
                .type(adapt(trade.getSide()))
                .originalAmount((adapt(trade.getSide()).equals(Order.OrderType.ASK) || adapt(trade.getSide()).equals(Order.OrderType.EXIT_BID)) ? trade.getSize().abs().negate() : trade.getSize().abs())
                .instrument(instrument)
                .price(trade.getPrice())
                .timestamp(new Date(trade.getTime()))
                .id(trade.getTid().toString())
                .build();
    }

    /**
     * Adapt Hyperliquid CandleSnapshot to XChange CandleStickData
     */

    public static Order.OrderType adapt(Side side) {
        switch (side) {
            case B:
                return Order.OrderType.BID;
            case A:
                return Order.OrderType.ASK;
            default:
                throw new RuntimeException("Not supported order side: " + side);
        }
    }

    public static CandleStickData adaptCandleStickData(HyperliquidCandleSnapshot candleSnapshot, Instrument instrument) {
        List<org.knowm.xchange.dto.marketdata.CandleStick> candleSticks = new ArrayList<>();

        if (candleSnapshot.getTimestamps() != null && !candleSnapshot.getTimestamps().isEmpty()) {
            for (int i = 0; i < candleSnapshot.getTimestamps().size(); i++) {
                if (i < candleSnapshot.getOpens().size() &&
                        i < candleSnapshot.getHighs().size() &&
                        i < candleSnapshot.getLows().size() &&
                        i < candleSnapshot.getCloses().size() &&
                        i < candleSnapshot.getVolumes().size()) {

                    org.knowm.xchange.dto.marketdata.CandleStick candleStick =
                            new org.knowm.xchange.dto.marketdata.CandleStick.Builder()
                                    .timestamp(new Date(candleSnapshot.getTimestamps().get(i)))
                                    .open(candleSnapshot.getOpens().get(i))
                                    .high(candleSnapshot.getHighs().get(i))
                                    .low(candleSnapshot.getLows().get(i))
                                    .close(candleSnapshot.getCloses().get(i))
                                    .volume(candleSnapshot.getVolumes().get(i))
                                    .build();
                    candleSticks.add(candleStick);
                }
            }
        }

        return new CandleStickData(instrument, candleSticks);
    }

    public static Instrument adaptInstrument(String instrumentName) {


        return new FuturesContract(new CurrencyPair(instrumentName, "USD"), "PERPETUAL");

    }


    public static Order adaptOrder(org.knowm.xchange.hyperliquid.dto.trade.Order order) {
        Order.OrderType type = adapt(order.getSide());
        Instrument instrument = adaptInstrument(order.getCoin());
        Order.Builder builder;
        builder = new LimitOrder.Builder(type, instrument).limitPrice(order.getPrice());

        builder

                .id(order.getOid())
                .userReference(Cloid.toMinimalHexString(order.getCloid()))
                .timestamp(new Date(order.getTimestamp()))
                .averagePrice(order.getPrice())
                .originalAmount(order.getOrigSz())
                .cumulativeAmount(order.getOrigSz().subtract(order.getSz()));

        return builder.build();
    }

    public static OpenOrders adaptOpenOrders(List<org.knowm.xchange.hyperliquid.dto.trade.Order> orders) {
        List<LimitOrder> limitOrders = new ArrayList<>();
        List<Order> otherOrders = new ArrayList<>();

        orders.forEach(
                o -> {
                    Order order = HyperliquidAdapters.adaptOrder(o);
                    if (order instanceof LimitOrder) {
                        limitOrders.add((LimitOrder) order);
                    } else {
                        otherOrders.add(order);
                    }
                });

        return new OpenOrders(limitOrders, otherOrders);
    }

    public static Order.OrderStatus adaptOrderStatus(String state) {

        switch (state.toLowerCase()) {
            case "open":
            case "untriggered":
                return Order.OrderStatus.OPEN;
            case "filled":
                return Order.OrderStatus.FILLED;
            case "rejected":
                return Order.OrderStatus.REJECTED;
            case "cancelled":
            case "canceled":
                return Order.OrderStatus.CANCELED;
            case "archive":
            default:
                return Order.OrderStatus.UNKNOWN;
        }
    }


    public static Cloid adaptUserReferenceToCoid(String userRef) {
        Cloid cloid=null;
        if (userRef != null && !userRef.isEmpty()) {
            // Handle hex string input - pad to 32 hex characters if needed
            String hexValue = userRef.startsWith("0x") ? userRef.substring(2) : userRef;
            // Pad to 32 characters (16 bytes)
            String paddedHex = String.format("%032x", new java.math.BigInteger(hexValue, 16));
            cloid = Cloid.fromStr("0x" + paddedHex);

        }
        return cloid;
    }

    // ========== Order Placement Helpers ==========

    /**
     * Create order wire in Hyperliquid format
     * Common method used by both REST and WebSocket order placement
     *
     * @param assetIndex Asset index from metadata
     * @param isBuy      True for buy, false for sell
     * @param limitPx    Limit price
     * @param size       Order size
     * @param reduceOnly True for reduce-only orders
     * @param orderType  Order type structure (GTC, IOC, ALO, etc.)
     * @param cloid      Client order ID (optional, must be 16-byte hex string)
     * @return Order wire Map ready for inclusion in action
     */
    public static Map<String, Object> createOrderWire(
            int assetIndex,
            boolean isBuy,
            double limitPx,
            double size,
            boolean reduceOnly,
            Map<String, Object> orderType,
            Cloid cloid) {

        Map<String, Object> orderWire = new LinkedHashMap<>();
        orderWire.put("a", assetIndex);                    // asset index
        orderWire.put("b", isBuy);                         // is_buy
        orderWire.put("p", floatToWire(limitPx));          // price as string
        orderWire.put("s", floatToWire(size));             // size as string
        orderWire.put("r", reduceOnly);                    // reduce_only
        orderWire.put("t", orderType);                     // order_type

        // Add client order ID if provided
        if (cloid != null) {
            orderWire.put("c", cloid.toRaw());
        }

        return orderWire;
    }

    /**
     * Create order action wrapper
     * Common method used by both REST and WebSocket order placement
     *
     * @param orderWires List of order wires
     * @param builder    Optional builder info
     * @return Action Map ready for signing
     */
    public static Map<String, Object> createOrderAction(
            java.util.List<Map<String, Object>> orderWires,
            Map<String, Object> builder) {

        Map<String, Object> action = new LinkedHashMap<>();
        action.put("type", "order");
        action.put("orders", orderWires);
        action.put("grouping", "na");

        // Add builder info if provided
        if (builder != null) {
            // Normalize builder address to lowercase (matching Python SDK)
            if (builder.containsKey("b")) {
                String builderAddress = (String) builder.get("b");
                if (builderAddress != null) {
                    builder.put("b", builderAddress.toLowerCase());
                }
            }
            action.put("builder", builder);
        }

        return action;
    }

    /**
     * Create signed request body for action
     * Common method used by both REST and WebSocket order placement
     *
     * @param action       The action to sign
     * @param nonce        Timestamp in milliseconds
     * @param signature    EIP-712 signature
     * @param vaultAddress Vault address (or null)
     * @param expiresAfter Expiration timestamp (or null)
     * @return Request body Map ready for API call
     */
    public static Map<String, Object> createSignedRequestBody(
            Map<String, Object> action,
            long nonce,
            Map<String, Object> signature,
            String vaultAddress,
            Long expiresAfter) {

        Map<String, Object> requestBody = new LinkedHashMap<>();
        requestBody.put("action", action);
        requestBody.put("nonce", nonce);
        requestBody.put("signature", signature);
        requestBody.put("vaultAddress", vaultAddress);
        requestBody.put("expiresAfter", expiresAfter);

        return requestBody;
    }

    /**
     * Create order type structure based on order type string
     * Common method used by both REST and WebSocket order placement
     *
     * @param orderType Type string: "gtc", "ioc", "alo", "limit", "market"
     * @return Order type Map
     */
    public static Map<String, Object> createOrderType(String orderType) {
        Map<String, Object> orderTypeMap = new LinkedHashMap<>();

        switch (orderType.toLowerCase()) {
            case "gtc":
            case "limit": {
                Map<String, Object> limitType = new LinkedHashMap<>();
                limitType.put("tif", "Gtc");
                orderTypeMap.put("limit", limitType);
                break;
            }
            case "ioc": {
                Map<String, Object> limitType = new LinkedHashMap<>();
                limitType.put("tif", "Ioc");
                orderTypeMap.put("limit", limitType);
                break;
            }
            case "alo": {
                Map<String, Object> limitType = new LinkedHashMap<>();
                limitType.put("tif", "Alo");
                orderTypeMap.put("limit", limitType);
                break;
            }
            case "market": {
                Map<String, Object> marketType = new LinkedHashMap<>();
                orderTypeMap.put("market", marketType);
                break;
            }
            default: {
                // Default to GTC limit order
                Map<String, Object> limitType = new LinkedHashMap<>();
                limitType.put("tif", "Gtc");
                orderTypeMap.put("limit", limitType);
                break;
            }
        }

        return orderTypeMap;
    }

    /**
     * Convert float to wire format matching Python SDK's float_to_wire
     * Ensures proper decimal normalization and validates rounding
     * See: hyperliquid/utils/signing.py lines 438-445
     *
     * @param x The number to format
     * @return Formatted string
     */
    public static String floatToWire(double x) {
        // Round to 8 decimal places
        String rounded = String.format(java.util.Locale.US, "%.8f", x);

        // Validate no significant rounding occurred (matching Python's 1e-12 threshold)
        double roundedValue = Double.parseDouble(rounded);
        if (Math.abs(roundedValue - x) >= 1e-12) {
            throw new IllegalArgumentException("float_to_wire causes rounding: " + x);
        }

        // Handle -0 case
        if (rounded.equals("-0") || rounded.equals("-0.00000000")) {
            rounded = "0.00000000";
        }

        // Normalize using BigDecimal (matching Python's Decimal.normalize())
        java.math.BigDecimal decimal = new java.math.BigDecimal(rounded);
        decimal = decimal.stripTrailingZeros();

        // Return plain string representation (no scientific notation)
        return decimal.toPlainString();
    }

    /**
     * Adapt Hyperliquid Fill to XChange UserTrade
     *
     * @param fill The Hyperliquid fill
     * @return XChange UserTrade
     */
    public static org.knowm.xchange.dto.trade.UserTrade adaptUserTrade(
            org.knowm.xchange.hyperliquid.dto.trade.Fill fill) {

        // Convert side (A=ask/sell, B=bid/buy)
        Order.OrderType type = fill.isBuy() ? Order.OrderType.BID : Order.OrderType.ASK;

        // Convert coin to instrument
        Instrument instrument = adaptInstrument(fill.getCoin());

        // Parse fee currency from feeToken (e.g., "USDC")
        org.knowm.xchange.currency.Currency feeCurrency = null;
        if (fill.getFeeToken() != null) {
            try {
                feeCurrency = org.knowm.xchange.currency.Currency.getInstance(fill.getFeeToken());
            } catch (Exception e) {
                // If currency parsing fails, leave as null
            }
        }

        return new org.knowm.xchange.dto.trade.UserTrade.Builder()
                .type(type)
                .originalAmount(fill.getSizeAsBigDecimal())
                .instrument(instrument)
                .price(fill.getPriceAsBigDecimal())
                .timestamp(new Date(fill.getTime()))
                .id(fill.getTradeId() != null ? fill.getTradeId().toString() : null)
                .orderId(fill.getOrderId() != null ? fill.getOrderId().toString() : null)
                .feeAmount(fill.getFeeAsBigDecimal())
                .feeCurrency(feeCurrency)
                .build();
    }

    /**
     * Adapt list of Hyperliquid Fills to list of XChange UserTrades
     *
     * @param fills List of Hyperliquid fills
     * @return List of XChange UserTrades
     */
    public static List<org.knowm.xchange.dto.trade.UserTrade> adaptUserTrades(
            List<org.knowm.xchange.hyperliquid.dto.trade.Fill> fills) {
        return fills.stream()
                .map(HyperliquidAdapters::adaptUserTrade)
                .collect(Collectors.toList());
    }

}