package org.knowm.xchange.examples.hyperliquid.marketdata;

import info.bitrich.xchangestream.core.StreamingExchange;
import io.reactivex.rxjava3.disposables.Disposable;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.examples.hyperliquid.HyperliquidDemoUtils;
import org.knowm.xchange.instrument.Instrument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Advanced demonstration of Hyperliquid streaming capabilities including:
 * - Multiple instruments streaming
 * - Mainnet vs Testnet comparison 
 * - Error handling and reconnection
 * - Statistics collection
 */
public class HyperliquidStreamingAdvancedDemo {

    private static final Logger LOG = LoggerFactory.getLogger(HyperliquidStreamingAdvancedDemo.class);

    public static void main(String[] args) throws InterruptedException {
        
        // Test both mainnet and testnet
        LOG.info("=== Hyperliquid Streaming Advanced Demo ===");
        
        // Run mainnet demo
        runStreamingDemo("MAINNET", false);
        
        Thread.sleep(2000); // Brief pause between demos
        
        // Run testnet demo
        runStreamingDemo("TESTNET", true);
    }
    
    private static void runStreamingDemo(String environment, boolean useTestnet) throws InterruptedException {
        LOG.info("\n=== {} Demo Starting ===", environment);
        
        // Create exchange
        StreamingExchange exchange = HyperliquidDemoUtils.createStreamingExchange(useTestnet);
        
        // Multiple instruments to test
        List<Instrument> instruments = Arrays.asList(
            CurrencyPair.BTC_USD,
            CurrencyPair.ETH_USD
        );
        
        List<Disposable> subscriptions = new ArrayList<>();
        
        // Statistics counters
        AtomicInteger tickerCount = new AtomicInteger(0);
        AtomicInteger orderBookCount = new AtomicInteger(0);
        AtomicInteger tradesCount = new AtomicInteger(0);
        
        try {
            // Connect to exchange
            LOG.info("Connecting to Hyperliquid {} WebSocket...", environment);
            exchange.connect().blockingAwait();
            LOG.info("Connected to {} successfully!", environment);
            
            // Subscribe to each instrument
            for (Instrument instrument : instruments) {
                String symbol = instrument.toString();
                
                // Subscribe to ticker (BBO)
                Disposable tickerSub = exchange
                    .getStreamingMarketDataService()
                    .getTicker(instrument)
                    .subscribe(
                        ticker -> {
                            tickerCount.incrementAndGet();
                            if (tickerCount.get() <= 3) { // Show first few messages
                                LOG.info("[{}] {} Ticker - Bid: {} @ {}, Ask: {} @ {}", 
                                    environment, symbol,
                                    ticker.getBid(), ticker.getBidSize(),
                                    ticker.getAsk(), ticker.getAskSize());
                            }
                        },
                        error -> LOG.error("[{}] Error in {} ticker stream: {}", environment, symbol, error.getMessage())
                    );
                subscriptions.add(tickerSub);
                
                // Subscribe to order book
                Disposable orderBookSub = exchange
                    .getStreamingMarketDataService()
                    .getOrderBook(instrument)
                    .subscribe(
                        orderBook -> {
                            orderBookCount.incrementAndGet();
                            if (orderBookCount.get() <= 3) { // Show first few messages
                                if (!orderBook.getBids().isEmpty() && !orderBook.getAsks().isEmpty()) {
                                    LOG.info("[{}] {} Order Book - Best Bid: {} @ {}, Best Ask: {} @ {}, Levels: {}/{}", 
                                        environment, symbol,
                                        orderBook.getBids().get(0).getLimitPrice(),
                                        orderBook.getBids().get(0).getOriginalAmount(),
                                        orderBook.getAsks().get(0).getLimitPrice(),
                                        orderBook.getAsks().get(0).getOriginalAmount(),
                                        orderBook.getBids().size(),
                                        orderBook.getAsks().size());
                                }
                            }
                        },
                        error -> LOG.error("[{}] Error in {} order book stream: {}", environment, symbol, error.getMessage())
                    );
                subscriptions.add(orderBookSub);
                
                // Subscribe to trades
                Disposable tradesSub = exchange
                    .getStreamingMarketDataService()
                    .getTrades(instrument)
                    .subscribe(
                        trade -> {
                            tradesCount.incrementAndGet();
                            if (tradesCount.get() <= 3) { // Show first few messages
                                LOG.info("[{}] {} Trade - {} {} @ {} at {}", 
                                    environment, symbol,
                                    trade.getType(),
                                    trade.getOriginalAmount(),
                                    trade.getPrice(),
                                    trade.getTimestamp());
                            }
                        },
                        error -> LOG.error("[{}] Error in {} trades stream: {}", environment, symbol, error.getMessage())
                    );
                subscriptions.add(tradesSub);
            }
            
            LOG.info("[{}] All subscriptions active. Collecting data for 20 seconds...", environment);
            
            // Let streams run for 20 seconds
            TimeUnit.SECONDS.sleep(20);
            
            // Print statistics
            LOG.info("\n=== {} Statistics ===", environment);
            LOG.info("Ticker messages received: {}", tickerCount.get());
            LOG.info("Order book messages received: {}", orderBookCount.get());
            LOG.info("Trade messages received: {}", tradesCount.get());
            LOG.info("Total messages: {}", tickerCount.get() + orderBookCount.get() + tradesCount.get());
            
        } catch (Exception e) {
            LOG.error("[{}] Error during streaming demo: {}", environment, e.getMessage(), e);
        } finally {
            // Clean up
            LOG.info("[{}] Cleaning up subscriptions...", environment);
            for (Disposable subscription : subscriptions) {
                if (subscription != null && !subscription.isDisposed()) {
                    subscription.dispose();
                }
            }
            
            // Disconnect
            LOG.info("[{}] Disconnecting...", environment);
            exchange.disconnect().blockingAwait();
            LOG.info("[{}] Disconnected successfully!", environment);
        }
    }
}