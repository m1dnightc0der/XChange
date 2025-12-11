package org.knowm.xchange.examples.hyperliquid.marketdata;

import org.knowm.xchange.Exchange;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.CandleStickData;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.examples.hyperliquid.HyperliquidDemoUtils;
import org.knowm.xchange.hyperliquid.service.HyperliquidMarketDataService;
import org.knowm.xchange.service.marketdata.MarketDataService;
import org.knowm.xchange.service.trade.params.DefaultCandleStickParam;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Demonstration of Hyperliquid REST API market data methods:
 * - getTicker() using allMids API
 * - getOrderBook() using l2Book API  
 * - getCandleStickData() using candleSnapshot API
 */
public class HyperliquidMarketdataDemo {

    public static void main(String[] args) throws IOException {
        
        System.out.println("=== Hyperliquid REST API Market Data Demo ===\n");
        
        // Create exchange using utility
        Exchange exchange = HyperliquidDemoUtils.createExchange();
        
        // Demonstrate generic API methods
        demonstrateGenericAPI(exchange);
        
        System.out.println("\n" + "==================================================" + "\n");
        
        // Demonstrate raw API access
        demonstrateRawAPI(exchange);
    }

    /**
     * Demonstrate the standard XChange MarketDataService API
     */
    private static void demonstrateGenericAPI(Exchange exchange) throws IOException {
        System.out.println("=== Generic XChange API Demo ===\n");
        
        MarketDataService marketDataService = exchange.getMarketDataService();
        
        // Test instruments
        List<CurrencyPair> instruments = Arrays.asList(
            CurrencyPair.BTC_USD,
            CurrencyPair.ETH_USD
        );
        
        for (CurrencyPair instrument : instruments) {
            System.out.println("--- " + instrument + " ---");
            
            try {
                // 1. Get Ticker (using allMids API)
                System.out.println("1. Getting Ticker...");
                Ticker ticker = marketDataService.getTicker(instrument);
                if (ticker != null) {
                    System.out.printf("   Ticker: Last=%.2f, Timestamp=%s%n", 
                        ticker.getLast(), ticker.getTimestamp());
                    if (ticker.getBid() != null) {
                        System.out.printf("   Bid=%.2f, Ask=%.2f%n", ticker.getBid(), ticker.getAsk());
                    }
                } else {
                    System.out.println("   No ticker data available for " + instrument);
                }
                
                // 2. Get Order Book (using l2Book API)
                System.out.println("2. Getting Order Book...");
                OrderBook orderBook = marketDataService.getOrderBook(instrument);
                if (orderBook != null && !orderBook.getBids().isEmpty() && !orderBook.getAsks().isEmpty()) {
                    System.out.printf("   Order Book: %d bids, %d asks%n", 
                        orderBook.getBids().size(), orderBook.getAsks().size());
                    System.out.printf("   Best Bid: %.2f @ %.6f%n", 
                        orderBook.getBids().get(0).getLimitPrice(),
                        orderBook.getBids().get(0).getOriginalAmount());
                    System.out.printf("   Best Ask: %.2f @ %.6f%n", 
                        orderBook.getAsks().get(0).getLimitPrice(),
                        orderBook.getAsks().get(0).getOriginalAmount());
                    System.out.printf("   Spread: %.2f%n", 
                        orderBook.getAsks().get(0).getLimitPrice().subtract(orderBook.getBids().get(0).getLimitPrice()));
                } else {
                    System.out.println("   No order book data available for " + instrument);
                }
                
                // 3. Get Candle Stick Data (using candleSnapshot API)
                System.out.println("3. Getting Candlestick Data (last 24 hours)...");
                Date endTime = Date.from(Instant.now());
                Date startTime = Date.from(Instant.now().minus(1, ChronoUnit.DAYS));
                DefaultCandleStickParam params = new DefaultCandleStickParam(startTime, endTime, 3600); // 1 hour intervals
                
                CandleStickData candleData = marketDataService.getCandleStickData(instrument, params);
                if (candleData != null && !candleData.getCandleSticks().isEmpty()) {
                    System.out.printf("   Candle Data: %d candles retrieved%n", candleData.getCandleSticks().size());
                    
                    // Show first and last candle
                    org.knowm.xchange.dto.marketdata.CandleStick firstCandle = candleData.getCandleSticks().get(0);
                    org.knowm.xchange.dto.marketdata.CandleStick lastCandle = candleData.getCandleSticks().get(candleData.getCandleSticks().size() - 1);
                    
                    System.out.printf("   First Candle: O=%.2f H=%.2f L=%.2f C=%.2f V=%.2f Time=%s%n",
                        firstCandle.getOpen(), firstCandle.getHigh(), firstCandle.getLow(), 
                        firstCandle.getClose(), firstCandle.getVolume(), firstCandle.getTimestamp());
                    System.out.printf("   Last Candle:  O=%.2f H=%.2f L=%.2f C=%.2f V=%.2f Time=%s%n",
                        lastCandle.getOpen(), lastCandle.getHigh(), lastCandle.getLow(), 
                        lastCandle.getClose(), lastCandle.getVolume(), lastCandle.getTimestamp());
                } else {
                    System.out.println("   No candle data available for " + instrument);
                }
                
            } catch (Exception e) {
                System.err.printf("   Error processing %s: %s%n", instrument, e.getMessage());
            }
            
            System.out.println();
        }
    }

    /**
     * Demonstrate raw Hyperliquid API access for advanced usage
     */
    private static void demonstrateRawAPI(Exchange exchange) throws IOException {
        System.out.println("=== Raw Hyperliquid API Demo ===\n");
        
        HyperliquidMarketDataService rawService = (HyperliquidMarketDataService) exchange.getMarketDataService();
        
        try {
            // 1. Raw AllMids API
            System.out.println("1. Raw AllMids API Call...");
            org.knowm.xchange.hyperliquid.dto.marketdata.HyperliquidAllMids allMids = rawService.getHyperliquidAllMids();
            if (allMids != null ) {
                System.out.printf("   Retrieved mid prices for %d coins%n", allMids.size());
                
                // Show first few mids
                allMids.getKeys().stream()
                    .limit(5)
                    .forEach(entry -> 
                        System.out.printf("   %s: %.2f%n", entry, allMids.getValue(entry))
                    );
            }
            
            // 2. Raw L2Book API
            System.out.println("\n2. Raw L2Book API Call for BTC...");
            org.knowm.xchange.hyperliquid.dto.marketdata.HyperliquidL2Book l2Book = rawService.getHyperliquidL2Book("BTC", null);
            if (l2Book != null) {
                System.out.printf("   L2 Book for %s: %d levels, timestamp=%d%n", 
                    l2Book.getCoin(), l2Book.getLevels().size(), l2Book.getTime());
                
                // Show first few levels
                l2Book.getLevels().stream()
                    .limit(3)
                    .forEach(level -> 
                        System.out.printf("   Level: Price=%.2f",
                            level)
                    );
            }
            
            // 3. Raw CandleSnapshot API
            System.out.println("\n3. Raw CandleSnapshot API Call for ETH...");
            org.knowm.xchange.hyperliquid.dto.marketdata.HyperliquidCandleSnapshot candleSnapshot = rawService.getHyperliquidCandleSnapshot("ETH", "1h", null, null);
            if (candleSnapshot != null) {
                System.out.printf("   Candle Snapshot status: %s%n", candleSnapshot.getStatus());
                if (candleSnapshot.getTimestamps() != null) {
                    System.out.printf("   Retrieved %d candles%n", candleSnapshot.getTimestamps().size());
                    
                    // Show summary of price range
                    if (!candleSnapshot.getHighs().isEmpty() && !candleSnapshot.getLows().isEmpty()) {
                        java.math.BigDecimal maxHigh = candleSnapshot.getHighs().stream().max(java.math.BigDecimal::compareTo).orElse(null);
                        java.math.BigDecimal minLow = candleSnapshot.getLows().stream().min(java.math.BigDecimal::compareTo).orElse(null);
                        System.out.printf("   Price range: %.2f - %.2f%n", minLow, maxHigh);
                    }
                }
            }
            
        } catch (Exception e) {
            System.err.println("   Error in raw API calls: " + e.getMessage());
            e.printStackTrace();
        }
    }
}