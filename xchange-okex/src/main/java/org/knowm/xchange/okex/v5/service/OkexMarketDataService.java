package org.knowm.xchange.okex.v5.service;

import org.knowm.xchange.client.ResilienceRegistries;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.CandleStickData;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Trades;
import org.knowm.xchange.exceptions.NotYetImplementedForExchangeException;
import org.knowm.xchange.instrument.Instrument;
import org.knowm.xchange.okex.v5.OkexAdapters;
import org.knowm.xchange.okex.v5.OkexExchange;
import org.knowm.xchange.okex.v5.dto.OkexResponse;
import org.knowm.xchange.okex.v5.dto.marketdata.OkexCandleStick;
import org.knowm.xchange.service.marketdata.MarketDataService;
import org.knowm.xchange.service.trade.params.CandleStickDataParams;
import org.knowm.xchange.service.trade.params.DefaultCandleStickParam;
import org.knowm.xchange.service.trade.params.DefaultCandleStickParamWithLimit;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/** Author: Max Gao (gaamox@tutanota.com) Created: 08-06-2021 */
public class OkexMarketDataService extends OkexMarketDataServiceRaw implements MarketDataService {
  public static final String SPOT = "SPOT";
  public static final String SWAP = "SWAP";
  public static final String FUTURES = "FUTURES";
  public static final String OPTION = "OPTION";

  public OkexMarketDataService(OkexExchange exchange, ResilienceRegistries resilienceRegistries) {
    super(exchange, resilienceRegistries);
  }

  @Override
  public OrderBook getOrderBook(Instrument instrument, Object... args) throws IOException {
    int limitDepth = 50;

    if (args != null && args.length == 1) {
      Object arg0 = args[0];
      if (!(arg0 instanceof Integer)) {
        throw new IllegalArgumentException("Argument 0 must be an Integer!");
      } else {
        limitDepth = (Integer) arg0;
      }
    }
    return OkexAdapters.adaptOrderBook(
        getOkexOrderbook(OkexAdapters.adaptCurrencyPairId(instrument), limitDepth), instrument);
  }

  @Override
  public OrderBook getOrderBook(CurrencyPair currencyPair, Object... args) throws IOException {
    return this.getOrderBook((Instrument) currencyPair, args);
  }

  @Override
  public Trades getTrades(Instrument instrument, Object... args) throws IOException {
    int limitTrades = 100;

    if (args != null && args.length == 1) {
      Object arg0 = args[0];
      if (!(arg0 instanceof Integer)) {
        throw new IllegalArgumentException("Argument 0 must be an Integer!");
      } else {
        limitTrades = (Integer) arg0;
      }
    }

    return OkexAdapters.adaptTrades(
        getOkexTrades(OkexAdapters.adaptCurrencyPairId(instrument), limitTrades).getData(),
        instrument);
  }

  @Override
  public Trades getTrades(CurrencyPair currencyPair, Object... args) throws IOException {
    return this.getTrades((Instrument) currencyPair, args);
  }


  @Override
  public CandleStickData getCandleStickData(CurrencyPair currencyPair, CandleStickDataParams params)
          throws IOException {

    if (!(params instanceof DefaultCandleStickParam)) {
      throw new NotYetImplementedForExchangeException("Only DefaultCandleStickParam is supported");
    }
    DefaultCandleStickParam defaultCandleStickParam = (DefaultCandleStickParam) params;
    OkexCandleStickPeriodType periodType =
            OkexCandleStickPeriodType.getPeriodTypeFromSecs(defaultCandleStickParam.getPeriodInSecs());
    if (periodType == null) {
      throw new NotYetImplementedForExchangeException("Only discrete period values are supported;" +
              Arrays.toString(OkexCandleStickPeriodType.getSupportedPeriodsInSecs()));
    }

    String limit = null;
    if (params instanceof DefaultCandleStickParamWithLimit) {
      limit = String.valueOf(((DefaultCandleStickParamWithLimit) params).getLimit());
    }

    OkexResponse<List<OkexCandleStick>> historyCandle = getHistoryCandle(
            OkexAdapters.adaptCurrencyPairId(currencyPair),
            String.valueOf(defaultCandleStickParam.getEndDate().getTime()),
            String.valueOf(defaultCandleStickParam.getStartDate().getTime()),
            periodType.getFieldValue(), limit);
    return OkexAdapters.adaptCandleStickData(historyCandle.getData(), currencyPair);
  }
}
