package org.knowm.xchange.deribit.v2.service;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.deribit.v2.DeribitAdapters;
import org.knowm.xchange.deribit.v2.DeribitExchange;
import org.knowm.xchange.deribit.v2.dto.DeribitException;
import org.knowm.xchange.deribit.v2.dto.marketdata.DeribitCandleStick;
import org.knowm.xchange.deribit.v2.dto.marketdata.DeribitOrderBook;
import org.knowm.xchange.deribit.v2.dto.marketdata.DeribitTicker;
import org.knowm.xchange.deribit.v2.dto.marketdata.DeribitTrades;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.marketdata.CandleStickData;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trades;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.exceptions.ExchangeException;
import org.knowm.xchange.exceptions.NotYetImplementedForExchangeException;
import org.knowm.xchange.instrument.Instrument;
import org.knowm.xchange.service.marketdata.MarketDataService;
import org.knowm.xchange.service.trade.params.CandleStickDataParams;
import org.knowm.xchange.service.trade.params.DefaultCandleStickParam;
import org.knowm.xchange.service.trade.params.DefaultCandleStickParamWithLimit;

/**
 * Implementation of the market data service for Bitmex
 *
 * <ul>
 *   <li>Provides access to various market data values
 * </ul>
 */
public class DeribitMarketDataService extends DeribitMarketDataServiceRaw
    implements MarketDataService {

  /**
   * Constructor
   *
   * @param exchange
   */
  public DeribitMarketDataService(DeribitExchange exchange) {

    super(exchange);
  }

  @Override
  public Ticker getTicker(Instrument instrument, Object... args) throws IOException {
    String deribitInstrumentName = DeribitAdapters.adaptInstrumentName(instrument);
    DeribitTicker deribitTicker;

    try {
      deribitTicker = super.getDeribitTicker(deribitInstrumentName);
    } catch (DeribitException ex) {
      throw DeribitAdapters.adapt(ex);
    }
    return DeribitAdapters.adaptTicker(deribitTicker);
  }

  @Override
  public OrderBook getOrderBook(Instrument instrument, Object... args) throws IOException {
    String deribitInstrumentName = DeribitAdapters.adaptInstrumentName(instrument);
    DeribitOrderBook deribitOrderBook;
    try {
      deribitOrderBook = super.getDeribitOrderBook(deribitInstrumentName, null);
    } catch (DeribitException ex) {
      throw new ExchangeException(ex);
    }

    return DeribitAdapters.adaptOrderBook(deribitOrderBook);
  }
  @Override
  public CandleStickData getCandleStickData(Instrument instrument, CandleStickDataParams params)
          throws IOException {

    if (!(params instanceof DefaultCandleStickParam)) {
      throw new NotYetImplementedForExchangeException("Only DefaultCandleStickParam is supported");
    }
    DefaultCandleStickParam defaultCandleStickParam = (DefaultCandleStickParam) params;
    DeribitCandleStickPeriodType periodType =
            DeribitCandleStickPeriodType.getPeriodTypeFromSecs(defaultCandleStickParam.getPeriodInSecs());
    if (periodType == null) {
      throw new NotYetImplementedForExchangeException(
              "Only discrete period values are supported;"
                      + Arrays.toString(DeribitCandleStickPeriodType.getSupportedPeriodsInSecs()));
    }

    String limit = null;
    if (params instanceof DefaultCandleStickParamWithLimit) {
      limit = String.valueOf(((DefaultCandleStickParamWithLimit) params).getLimit());
    }

    DeribitCandleStick historyCandle =
            getHistoryCandle(
                    DeribitAdapters.adaptInstrumentName(instrument),
                    String.valueOf(defaultCandleStickParam.getStartDate().getTime()),
                    String.valueOf(defaultCandleStickParam.getEndDate().getTime()),
                    periodType.getFieldValue(),
                    limit);
    return DeribitAdapters.adaptCandleSticks(historyCandle, instrument);
  }
  @Override
  public Trades getTrades(Instrument instrument, Object... args) throws IOException {
    String deribitInstrumentName = DeribitAdapters.adaptInstrumentName(instrument);
    DeribitTrades deribitTrades;

    try {
      deribitTrades =
          super.getLastTradesByInstrument(deribitInstrumentName, null, null, null, null, null);
    } catch (DeribitException ex) {
      throw new ExchangeException(ex);
    }

    return DeribitAdapters.adaptTrades(deribitTrades, instrument);
  }

  public static OrderBook convertOrderBook(DeribitOrderBook ob, Instrument pair) {
    List<LimitOrder> bids =
        ob.getBids().entrySet().stream()
            .map(e -> new LimitOrder(Order.OrderType.BID, e.getValue(), pair, null, null, e.getKey()))
            .collect(Collectors.toList());
    List<LimitOrder> asks =
        ob.getAsks().entrySet().stream()
            .map(e -> new LimitOrder(Order.OrderType.ASK, e.getValue(), pair, null, null, e.getKey()))
            .collect(Collectors.toList());
    return new OrderBook(Date.from(Instant.now()), asks, bids);
  }
}
