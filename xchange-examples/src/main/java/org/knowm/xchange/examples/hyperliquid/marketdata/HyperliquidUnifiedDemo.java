package org.knowm.xchange.examples.hyperliquid.marketdata;

import info.bitrich.xchangestream.core.StreamingExchange;
import io.reactivex.rxjava3.disposables.Disposable;
import org.knowm.xchange.Exchange;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.CandleStickData;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.examples.hyperliquid.HyperliquidDemoUtils;
import org.knowm.xchange.service.marketdata.MarketDataService;
import org.knowm.xchange.service.trade.params.DefaultCandleStickParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unified demonstration showing both REST and Streaming APIs working together
 * This example shows how to:
 * - Use REST API for historical data and snapshots
 * - Use Streaming API for real-time updates
 * - Combine both for comprehensive market data coverage
 */
public class HyperliquidUnifiedDemo {

    private static final Logger LOG = LoggerFactory.getLogger(HyperliquidUnifiedDemo.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("=== Hyperliquid Unified REST + Streaming Demo ===\n");
        
        CurrencyPair instrument = CurrencyPair.BTC_USD;
        
        // Create both exchanges
        Exchange restExchange = HyperliquidDemoUtils.createExchange(false);
        StreamingExchange streamingExchange = HyperliquidDemoUtils.createStreamingExchange(false);
        
        System.out.println("Created REST and Streaming exchanges for: " + instrument);
        System.out.println();
        
        // Phase 1: Get historical context using REST
        getHistoricalContext(restExchange, instrument);
        
        System.out.println("\n" + "==================================================" + "\n");
        
        // Phase 2: Start real-time streaming
        startRealTimeStreaming(streamingExchange, instrument);
        
        System.out.println("\n" + "==================================================" + "\n");
        
        // Phase 3: Demonstrate hybrid usage
        demonstrateHybridUsage(restExchange, streamingExchange, instrument);
    }
    
    private static void getHistoricalContext(Exchange exchange, CurrencyPair instrument) throws IOException {
        System.out.println("=== Phase 1: Historical Context via REST API ===\n");
        
        MarketDataService marketData = exchange.getMarketDataService();
        
        // 1. Current market snapshot
        System.out.println("1. Current Market Snapshot:");
        Ticker currentTicker = marketData.getTicker(instrument);
        if (currentTicker != null && currentTicker.getLast() != null) {
            System.out.printf("   Current Price: $%.2f%n", currentTicker.getLast());
            System.out.printf("   Timestamp: %s%n", currentTicker.getTimestamp());
        }
        
        OrderBook currentOrderBook = marketData.getOrderBook(instrument);
        if (currentOrderBook != null && !currentOrderBook.getBids().isEmpty()) {
            System.out.printf("   Best Bid: $%.2f (%.6f)%n", 
                currentOrderBook.getBids().get(0).getLimitPrice(),
                currentOrderBook.getBids().get(0).getOriginalAmount());
            System.out.printf("   Best Ask: $%.2f (%.6f)%n", 
                currentOrderBook.getAsks().get(0).getLimitPrice(),
                currentOrderBook.getAsks().get(0).getOriginalAmount());
        }
        
        // 2. Historical price data
        System.out.println("\n2. Historical Price Data (Last 24 Hours):");
        Date endTime = Date.from(Instant.now());
        Date startTime = Date.from(Instant.now().minus(1, ChronoUnit.DAYS));
        DefaultCandleStickParam params = new DefaultCandleStickParam(startTime, endTime, 3600);
        
        CandleStickData candleData = marketData.getCandleStickData(instrument, params);
        if (candleData != null && !candleData.getCandleSticks().isEmpty()) {
            java.util.List<org.knowm.xchange.dto.marketdata.CandleStick> candles = candleData.getCandleSticks();
            org.knowm.xchange.dto.marketdata.CandleStick firstCandle = candles.get(0);
            org.knowm.xchange.dto.marketdata.CandleStick lastCandle = candles.get(candles.size() - 1);
            
            System.out.printf("   Retrieved %d hourly candles%n", candles.size());
            System.out.printf("   24h Range: $%.2f - $%.2f%n", 
                candles.stream().map(c -> c.getLow()).min((a,b) -> a.compareTo(b)).orElse(null),
                candles.stream().map(c -> c.getHigh()).max((a,b) -> a.compareTo(b)).orElse(null));
            System.out.printf("   24h Change: $%.2f (%.2f%%)%n", 
                lastCandle.getClose().subtract(firstCandle.getOpen()),
                lastCandle.getClose().subtract(firstCandle.getOpen()).divide(firstCandle.getOpen()).multiply(java.math.BigDecimal.valueOf(100)));
        }
    }
    
    private static void startRealTimeStreaming(StreamingExchange exchange, CurrencyPair instrument) throws InterruptedException {
        System.out.println("=== Phase 2: Real-Time Streaming ===\n");
        
        // Connect to streaming
        System.out.println("Connecting to WebSocket...");
        exchange.connect().blockingAwait();
        System.out.println("Connected successfully!\n");
        
        // Counters for statistics
        AtomicInteger tickerUpdates = new AtomicInteger(0);
        AtomicInteger orderBookUpdates = new AtomicInteger(0);
        AtomicInteger tradeUpdates = new AtomicInteger(0);
        
        // Start streaming subscriptions
        Disposable tickerSubscription = exchange
            .getStreamingMarketDataService()
            .getTicker(instrument)
            .subscribe(
                ticker -> {
                    int count = tickerUpdates.incrementAndGet();
                    if (count <= 5) { // Show first 5 updates
                        LOG.info("Ticker Update #{}: Bid=${} Ask=${}", count, ticker.getBid(), ticker.getAsk());
                    }
                },
                error -> LOG.error("Ticker stream error", error)
            );
        
        Disposable orderBookSubscription = exchange
            .getStreamingMarketDataService()
            .getOrderBook(instrument)
            .subscribe(
                orderBook -> {
                    int count = orderBookUpdates.incrementAndGet();
                    if (count <= 3) { // Show first 3 updates
                        if (!orderBook.getBids().isEmpty() && !orderBook.getAsks().isEmpty()) {
                            LOG.info("OrderBook Update #{}: Spread=${}", count, 
                                orderBook.getAsks().get(0).getLimitPrice().subtract(orderBook.getBids().get(0).getLimitPrice()));
                        }
                    }
                },
                error -> LOG.error("OrderBook stream error", error)
            );
        
        Disposable tradesSubscription = exchange
            .getStreamingMarketDataService()
            .getTrades(instrument)
            .subscribe(
                trade -> {
                    int count = tradeUpdates.incrementAndGet();
                    if (count <= 5) { // Show first 5 trades
                        LOG.info("Trade #{}: {} {} @ ${}", count, trade.getType(), trade.getOriginalAmount(), trade.getPrice());
                    }
                },
                error -> LOG.error("Trades stream error", error)
            );
        
        // Let streams run for 15 seconds
        System.out.println("Streaming live data for 15 seconds...");
        TimeUnit.SECONDS.sleep(15);
        
        // Print statistics
        System.out.printf("\nStreaming Statistics:%n");
        System.out.printf("  Ticker Updates: %d%n", tickerUpdates.get());
        System.out.printf("  OrderBook Updates: %d%n", orderBookUpdates.get());
        System.out.printf("  Trade Updates: %d%n", tradeUpdates.get());
        System.out.printf("  Total Messages: %d%n", 
            tickerUpdates.get() + orderBookUpdates.get() + tradeUpdates.get());
        
        // Cleanup
        tickerSubscription.dispose();
        orderBookSubscription.dispose();
        tradesSubscription.dispose();
        exchange.disconnect().blockingAwait();
        System.out.println("Disconnected from streaming.");
    }
    
    private static void demonstrateHybridUsage(Exchange restExchange, StreamingExchange streamingExchange, 
                                              CurrencyPair instrument) throws IOException, InterruptedException {
        System.out.println("=== Phase 3: Hybrid REST + Streaming Usage ===\n");
        
        System.out.println("Scenario: Price monitoring with historical context");
        System.out.println("- Use REST for current snapshot and historical data");
        System.out.println("- Use Streaming for real-time price alerts");
        System.out.println();
        
        // Get current price from REST
        MarketDataService restService = restExchange.getMarketDataService();
        Ticker currentTicker = restService.getTicker(instrument);
        
        if (currentTicker == null || currentTicker.getLast() == null) {
            System.out.println("Unable to get current price data. Skipping hybrid demo.");
            return;
        }
        
        java.math.BigDecimal basePrice = currentTicker.getLast();
        java.math.BigDecimal alertThreshold = java.math.BigDecimal.valueOf(0.5); // 0.5% change
        
        System.out.printf("Base Price: $%.2f%n", basePrice);
        System.out.printf("Alert Threshold: ±%.1f%%%n", alertThreshold);
        System.out.println();
        
        // Setup streaming for real-time alerts
        streamingExchange.connect().blockingAwait();
        
        final AtomicInteger alertCount = new AtomicInteger(0);
        
        Disposable priceMonitor = streamingExchange
            .getStreamingMarketDataService()
            .getTicker(instrument)
            .subscribe(
                ticker -> {
                    if (ticker.getLast() != null) {
                        java.math.BigDecimal currentPrice = ticker.getLast();
                        java.math.BigDecimal change = currentPrice.subtract(basePrice);
                        java.math.BigDecimal changePercent = change.divide(basePrice).multiply(java.math.BigDecimal.valueOf(100));
                        
                        if (changePercent.abs().compareTo(alertThreshold) > 0) {
                            int count = alertCount.incrementAndGet();
                            LOG.warn("PRICE ALERT #{}: ${} ({:+.2f}%)", count, currentPrice, changePercent);
                            
                            if (count >= 3) {
                                LOG.info("Demo limit reached. Stopping monitoring.");
                            }
                        }
                    }
                },
                error -> LOG.error("Price monitoring error", error)
            );
        
        // Monitor for 30 seconds or until 3 alerts
        System.out.println("Monitoring for price changes... (30 seconds max)");
        for (int i = 0; i < 30 && alertCount.get() < 3; i++) {
            TimeUnit.SECONDS.sleep(1);
        }
        
        priceMonitor.dispose();
        streamingExchange.disconnect().blockingAwait();
        
        System.out.printf("\nMonitoring complete. Generated %d alerts.%n", alertCount.get());
        
        // Final REST snapshot for comparison
        System.out.println("\nFinal snapshot via REST:");
        Ticker finalTicker = restService.getTicker(instrument);
        if (finalTicker != null && finalTicker.getLast() != null) {
            java.math.BigDecimal finalChange = finalTicker.getLast().subtract(basePrice);
            java.math.BigDecimal finalChangePercent = finalChange.divide(basePrice).multiply(java.math.BigDecimal.valueOf(100));
            System.out.printf("Final Price: $%.2f (%+.2f%%)%n", finalTicker.getLast(), finalChangePercent);
        }
    }
}