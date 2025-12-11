package org.knowm.xchange.hyperliquid.service;

import org.knowm.xchange.dto.marketdata.CandleStickData;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trades;
import org.knowm.xchange.exceptions.NotYetImplementedForExchangeException;
import org.knowm.xchange.hyperliquid.HyperliquidAdapters;
import org.knowm.xchange.hyperliquid.HyperliquidExchange;
import org.knowm.xchange.hyperliquid.dto.marketdata.HyperliquidAllMids;
import org.knowm.xchange.hyperliquid.dto.marketdata.HyperliquidL2Book;
import org.knowm.xchange.hyperliquid.dto.marketdata.HyperliquidCandleSnapshot;
import org.knowm.xchange.instrument.Instrument;
import org.knowm.xchange.service.marketdata.MarketDataService;
import org.knowm.xchange.service.trade.params.CandleStickDataParams;

import java.io.IOException;
import java.util.Date;

/**
 * Implementation of the market data service for Hyperliquid
 *
 * <ul>
 *   <li>Provides access to various market data values
 * </ul>
 */
public class HyperliquidMarketDataService extends HyperliquidMarketDataServiceRaw
    implements MarketDataService {

  /**
   * Constructor
   *
   * @param exchange
   */
  public HyperliquidMarketDataService(HyperliquidExchange exchange) {
    super(exchange);
  }

  @Override
  public Ticker getTicker(Instrument instrument, Object... args) throws IOException {
    // Use allMids API to get ticker data
    HyperliquidAllMids allMids = getAllMids();
    return HyperliquidAdapters.adaptTicker(allMids, instrument);
  }

  @Override
  public OrderBook getOrderBook(Instrument instrument, Object... args) throws IOException {
    String coin = HyperliquidAdapters.adaptInstrumentToCoin(instrument);

    
    HyperliquidL2Book l2Book = getL2Book(coin, null);
    return HyperliquidAdapters.adaptOrderBook(l2Book,instrument);
  }

  @Override
  public CandleStickData getCandleStickData(Instrument instrument, CandleStickDataParams params) throws IOException {
    String coin = HyperliquidAdapters.adaptInstrumentToCoin(instrument);
    
    // Default parameters
    String interval = "1h"; // Default to 1 hour
    Long startTime = null;
    Long endTime = null;
    
    // Extract parameters from CandleStickDataParams if provided
    if (params != null) {
      // Determine interval based on params - this is a simplified mapping
      // In a real implementation, you'd need to properly map XChange intervals to Hyperliquid intervals
      interval = determineHyperliquidInterval(params);
      
      // Extract time range if available
      if (params instanceof org.knowm.xchange.service.trade.params.TradeHistoryParamsTimeSpan) {
        org.knowm.xchange.service.trade.params.TradeHistoryParamsTimeSpan timeSpan = 
            (org.knowm.xchange.service.trade.params.TradeHistoryParamsTimeSpan) params;
        
        if (timeSpan.getStartTime() != null) {
          startTime = timeSpan.getStartTime().getTime();
        }
        if (timeSpan.getEndTime() != null) {
          endTime = timeSpan.getEndTime().getTime();
        }
      }
    }
    
    HyperliquidCandleSnapshot candleSnapshot = getCandleSnapshot(coin, interval, startTime, endTime);
    return HyperliquidAdapters.adaptCandleStickData(candleSnapshot, instrument);
  }

  @Override
  public Trades getTrades(Instrument instrument, Object... args) throws IOException {
   return null;
  }

  /**
   * Helper method to determine Hyperliquid interval from XChange CandleStickDataParams
   * This is a simplified implementation - you may need to enhance this based on your needs
   */
  private String determineHyperliquidInterval(CandleStickDataParams params) {
    // Default implementation - you can enhance this based on the specific params structure
    // Hyperliquid supports: "1m", "15m", "1h", "4h", "1d"
    return "1h";
  }

  // Expose raw methods for advanced usage
  public HyperliquidAllMids getHyperliquidAllMids() throws IOException {
    return getAllMids();
  }

  public HyperliquidL2Book getHyperliquidL2Book(String coin, Integer nSigFigs) throws IOException {
    return getL2Book(coin, nSigFigs);
  }

  public HyperliquidCandleSnapshot getHyperliquidCandleSnapshot(String coin, String interval, Long startTime, Long endTime) throws IOException {
    return getCandleSnapshot(coin, interval, startTime, endTime);
  }
}