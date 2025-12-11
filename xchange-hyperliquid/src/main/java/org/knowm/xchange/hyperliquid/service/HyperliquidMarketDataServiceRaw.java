package org.knowm.xchange.hyperliquid.service;

import org.knowm.xchange.hyperliquid.HyperliquidExchange;
import org.knowm.xchange.hyperliquid.dto.marketdata.HyperliquidAllMids;
import org.knowm.xchange.hyperliquid.dto.marketdata.HyperliquidL2Book;
import org.knowm.xchange.hyperliquid.dto.marketdata.HyperliquidCandleSnapshot;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Raw implementation of market data service for Hyperliquid
 * Provides direct access to Hyperliquid API responses
 */
public class HyperliquidMarketDataServiceRaw extends HyperliquidBaseService {

    public HyperliquidMarketDataServiceRaw(HyperliquidExchange exchange) {
        super(exchange);
    }

    /**
     * Get mid prices for all coins using allMids API
     */
    public HyperliquidAllMids getAllMids() throws IOException {
        Map<String, Object> request = new HashMap<>();
        request.put("type", "allMids");


        return hyperliquid.getAllMids("application/json",request);
    }

    /**
     * Get L2 order book for a specific coin
     */
    public HyperliquidL2Book getL2Book(String coin, Integer nSigFigs) throws IOException {
        Map<String, Object> request = new HashMap<>();
        request.put("type", "l2Book");
        request.put("coin", coin);
        if (nSigFigs != null) {
            request.put("nSigFigs", nSigFigs);
        }
        
        return hyperliquid.getL2Book("application/json", request);
    }

    /**
     * Get candlestick data for a specific coin and interval
     */
    public HyperliquidCandleSnapshot getCandleSnapshot(String coin, String interval, Long startTime, Long endTime) throws IOException {
        Map<String, Object> request = new HashMap<>();
        request.put("type", "candleSnapshot");
        request.put("coin", coin);
        request.put("interval", interval);
        if (startTime != null) {
            request.put("startTime", startTime);
        }
        if (endTime != null) {
            request.put("endTime", endTime);
        }
        
        return hyperliquid.getCandleSnapshot("application/json", request);
    }
}