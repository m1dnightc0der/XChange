package org.knowm.xchange.hyperliquid;

import org.junit.Test;
import org.knowm.xchange.Exchange;
import org.knowm.xchange.ExchangeFactory;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.CandleStickData;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.service.marketdata.MarketDataService;
import org.knowm.xchange.service.trade.params.DefaultCandleStickParam;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class HyperliquidMarketDataIntegration {

  @Test
  public void testGetTicker() throws Exception {
    Exchange exchange = ExchangeFactory.INSTANCE.createExchange(HyperliquidExchange.class);
    MarketDataService marketDataService = exchange.getMarketDataService();

    Ticker ticker = marketDataService.getTicker(CurrencyPair.BTC_USD);
    System.out.println("BTC Ticker: " + ticker);
    
    if (ticker != null) {
      assertThat(ticker.getInstrument()).isEqualTo(CurrencyPair.BTC_USD);
      assertThat(ticker.getLast()).isNotNull();
    }
  }

  @Test
  public void testGetOrderBook() throws Exception {
    Exchange exchange = ExchangeFactory.INSTANCE.createExchange(HyperliquidExchange.class);
    MarketDataService marketDataService = exchange.getMarketDataService();

    OrderBook orderBook = marketDataService.getOrderBook(CurrencyPair.BTC_USD);
    System.out.println("BTC Order Book: " + orderBook);
    
    if (orderBook != null) {
      System.out.println("Order book has " + orderBook.getBids().size() + " bids and " + orderBook.getAsks().size() + " asks");
    }
  }

  @Test
  public void testGetCandleStickData() throws Exception {
    Exchange exchange = ExchangeFactory.INSTANCE.createExchange(HyperliquidExchange.class);
    MarketDataService marketDataService = exchange.getMarketDataService();

    // Get last 24 hours of data
    Date endTime = Date.from(Instant.now());
    Date startTime = Date.from(Instant.now().minus(1, ChronoUnit.DAYS));
    DefaultCandleStickParam params = new DefaultCandleStickParam(startTime, endTime, 3600); // 1 hour intervals

    CandleStickData candleData = marketDataService.getCandleStickData(CurrencyPair.BTC_USD, params);
    System.out.println("BTC Candle Data: " + candleData);
    
    if (candleData != null && !candleData.getCandleSticks().isEmpty()) {
      System.out.println("Retrieved " + candleData.getCandleSticks().size() + " candles");
      assertThat(candleData.getInstrument()).isEqualTo(CurrencyPair.BTC_USD);
    }
  }

  @Test
  public void testTestnetExchange() throws Exception {
    Exchange exchange = ExchangeFactory.INSTANCE.createExchange(HyperliquidExchange.class);
    exchange.getExchangeSpecification().setExchangeSpecificParametersItem("Use_Sandbox", true);
    exchange.applySpecification(exchange.getExchangeSpecification());
    
    MarketDataService marketDataService = exchange.getMarketDataService();

    Ticker ticker = marketDataService.getTicker(CurrencyPair.BTC_USD);
    System.out.println("Testnet BTC Ticker: " + ticker);
    
    // Testnet may have different behavior, so we just ensure it doesn't throw exceptions
  }
}