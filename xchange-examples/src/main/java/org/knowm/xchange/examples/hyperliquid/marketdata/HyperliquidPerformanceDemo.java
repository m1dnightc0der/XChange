package org.knowm.xchange.examples.hyperliquid.marketdata;

import org.knowm.xchange.Exchange;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.examples.hyperliquid.HyperliquidDemoUtils;
import org.knowm.xchange.hyperliquid.service.HyperliquidMarketDataService;
import org.knowm.xchange.service.marketdata.MarketDataService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

/**
 * Performance testing demonstration for Hyperliquid REST API
 * Tests:
 * - Latency measurements for different API calls
 * - Throughput testing
 * - Concurrent request handling
 * - Error rates under load
 */
public class HyperliquidPerformanceDemo {

    private static final int WARMUP_REQUESTS = 5;
    private static final int TEST_REQUESTS = 20;
    private static final int CONCURRENT_THREADS = 5;

    public static void main(String[] args) throws IOException {
        System.out.println("=== Hyperliquid REST API Performance Demo ===\n");
        
        Exchange exchange = HyperliquidDemoUtils.createExchange(false);
        MarketDataService marketData = exchange.getMarketDataService();
        
        System.out.println("Testing against: " + exchange.getExchangeSpecification().getSslUri());
        System.out.println("Warmup requests: " + WARMUP_REQUESTS);
        System.out.println("Test requests: " + TEST_REQUESTS);
        System.out.println("Concurrent threads: " + CONCURRENT_THREADS);
        System.out.println();
        
        // Run performance tests
        testSingleThreadedPerformance(marketData);
        System.out.println();
        testConcurrentPerformance(marketData);
        System.out.println();
        testThroughputMeasurement(marketData);
    }
    
    private static void testSingleThreadedPerformance(MarketDataService marketData) {
        System.out.println("=== Single-Threaded Performance Test ===");
        
        CurrencyPair testInstrument = CurrencyPair.BTC_USD;
        
        // Test each API method
        List<String> methods = Arrays.asList("getTicker", "getOrderBook", "getAllMids");
        
        for (String method : methods) {
            System.out.printf("\nTesting %s():n", method);
            
            // Warmup
            System.out.print("  Warming up... ");
            for (int i = 0; i < WARMUP_REQUESTS; i++) {
                try {
                    callMethod(marketData, method, testInstrument);
                } catch (Exception e) {
                    // Ignore warmup errors
                }
            }
            System.out.println("done");
            
            // Actual test
            List<Long> latencies = new ArrayList<>();
            int errors = 0;
            
            for (int i = 0; i < TEST_REQUESTS; i++) {
                long startTime = System.nanoTime();
                try {
                    Object result = callMethod(marketData, method, testInstrument);
                    long latency = (System.nanoTime() - startTime) / 1_000_000; // Convert to milliseconds
                    latencies.add(latency);
                    
                    if (result == null) {
                        errors++;
                    }
                } catch (Exception e) {
                    errors++;
                }
            }
            
            // Calculate statistics
            if (!latencies.isEmpty()) {
                LongSummaryStatistics stats = latencies.stream().mapToLong(Long::longValue).summaryStatistics();
                
                System.out.printf("  Results: %d successful, %d errors%n", latencies.size(), errors);
                System.out.printf("  Latency: avg=%.1fms, min=%dms, max=%dms%n", 
                    stats.getAverage(), stats.getMin(), stats.getMax());
                
                // Calculate percentiles
                latencies.sort(Long::compareTo);
                int p50Index = (int) (latencies.size() * 0.5);
                int p95Index = (int) (latencies.size() * 0.95);
                System.out.printf("  Percentiles: p50=%dms, p95=%dms%n", 
                    latencies.get(p50Index), latencies.get(p95Index));
            } else {
                System.out.printf("  Results: 0 successful, %d errors%n", errors);
            }
        }
    }
    
    private static void testConcurrentPerformance(MarketDataService marketData) {
        System.out.println("=== Concurrent Performance Test ===");
        
        CurrencyPair testInstrument = CurrencyPair.BTC_USD;
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_THREADS);
        
        try {
            // Test concurrent getTicker calls
            System.out.printf("Testing %d concurrent getTicker() calls...%n", CONCURRENT_THREADS);
            
            long startTime = System.currentTimeMillis();
            
            List<CompletableFuture<Long>> futures = new ArrayList<>();
            for (int i = 0; i < CONCURRENT_THREADS; i++) {
                CompletableFuture<Long> future = CompletableFuture.supplyAsync(() -> {
                    try {
                        long requestStart = System.nanoTime();
                        Ticker ticker = marketData.getTicker(testInstrument);
                        long latency = (System.nanoTime() - requestStart) / 1_000_000;
                        return ticker != null ? latency : -1L;
                    } catch (Exception e) {
                        return -1L;
                    }
                }, executor);
                futures.add(future);
            }
            
            // Wait for all requests to complete
            List<Long> results = new ArrayList<>();
            int errors = 0;
            
            for (CompletableFuture<Long> future : futures) {
                try {
                    Long result = future.get();
                    if (result >= 0) {
                        results.add(result);
                    } else {
                        errors++;
                    }
                } catch (Exception e) {
                    errors++;
                }
            }
            
            long totalTime = System.currentTimeMillis() - startTime;
            
            System.out.printf("  Total execution time: %dms%n", totalTime);
            System.out.printf("  Successful requests: %d, Errors: %d%n", results.size(), errors);
            
            if (!results.isEmpty()) {
                double avgLatency = results.stream().mapToLong(Long::longValue).average().orElse(0);
                System.out.printf("  Average latency: %.1fms%n", avgLatency);
                System.out.printf("  Requests per second: %.1f%n", (double) results.size() * 1000 / totalTime);
            }
            
        } finally {
            executor.shutdown();
        }
    }
    
    private static void testThroughputMeasurement(MarketDataService marketData) {
        System.out.println("=== Throughput Measurement ===");
        
        if (!(marketData instanceof HyperliquidMarketDataService)) {
            System.out.println("Skipping raw API throughput test (service type not supported)");
            return;
        }
        
        HyperliquidMarketDataService rawService = (HyperliquidMarketDataService) marketData;
        
        // Test sustained throughput over time
        int testDurationSeconds = 10;
        System.out.printf("Testing sustained throughput over %d seconds...%n", testDurationSeconds);
        
        long startTime = System.currentTimeMillis();
        long endTime = startTime + (testDurationSeconds * 1000L);
        
        int requestCount = 0;
        int errorCount = 0;
        List<Long> latencies = new ArrayList<>();
        
        while (System.currentTimeMillis() < endTime) {
            try {
                long requestStart = System.nanoTime();
                org.knowm.xchange.hyperliquid.dto.marketdata.HyperliquidAllMids allMids = rawService.getHyperliquidAllMids();
                long latency = (System.nanoTime() - requestStart) / 1_000_000;
                
                if (allMids != null) {
                    latencies.add(latency);
                } else {
                    errorCount++;
                }
                requestCount++;
                
                // Small delay to prevent overwhelming the API
                Thread.sleep(50);
                
            } catch (Exception e) {
                errorCount++;
                requestCount++;
            }
        }
        
        long actualDuration = System.currentTimeMillis() - startTime;
        
        System.out.printf("  Test duration: %dms%n", actualDuration);
        System.out.printf("  Total requests: %d%n", requestCount);
        System.out.printf("  Successful: %d, Errors: %d (%.1f%% error rate)%n", 
            latencies.size(), errorCount, (double) errorCount * 100 / requestCount);
        System.out.printf("  Throughput: %.2f requests/second%n", 
            (double) requestCount * 1000 / actualDuration);
        
        if (!latencies.isEmpty()) {
            LongSummaryStatistics stats = latencies.stream().mapToLong(Long::longValue).summaryStatistics();
            System.out.printf("  Latency stats: avg=%.1fms, min=%dms, max=%dms%n", 
                stats.getAverage(), stats.getMin(), stats.getMax());
        }
    }
    
    private static Object callMethod(MarketDataService marketData, String methodName, CurrencyPair instrument) throws IOException {
        switch (methodName) {
            case "getTicker":
                return marketData.getTicker(instrument);
            case "getOrderBook":
                return marketData.getOrderBook(instrument);
            case "getAllMids":
                if (marketData instanceof HyperliquidMarketDataService) {
                    return ((HyperliquidMarketDataService) marketData).getHyperliquidAllMids();
                }
                return null;
            default:
                throw new IllegalArgumentException("Unknown method: " + methodName);
        }
    }
}