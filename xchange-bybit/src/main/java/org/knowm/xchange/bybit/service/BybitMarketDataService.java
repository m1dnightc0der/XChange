package org.knowm.xchange.bybit.service;

import org.knowm.xchange.bybit.BybitAdapters;
import org.knowm.xchange.bybit.BybitExchange;
import org.knowm.xchange.bybit.dto.BybitCategory;
import org.knowm.xchange.bybit.dto.BybitResult;
import org.knowm.xchange.bybit.dto.marketdata.orderbooks.BybitOrderbook;
import org.knowm.xchange.bybit.dto.marketdata.tickers.BybitTicker;
import org.knowm.xchange.bybit.dto.marketdata.tickers.BybitTickers;
import org.knowm.xchange.bybit.dto.marketdata.tickers.linear.BybitLinearInverseTicker;
import org.knowm.xchange.bybit.dto.marketdata.tickers.option.BybitOptionTicker;
import org.knowm.xchange.bybit.dto.marketdata.tickers.spot.BybitSpotTicker;
import org.knowm.xchange.bybit.dto.marketdata.trades.BybitTrade;
import org.knowm.xchange.bybit.dto.marketdata.trades.BybitTradeResponse;
import org.knowm.xchange.client.ResilienceRegistries;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trades;
import org.knowm.xchange.exceptions.NotYetImplementedForExchangeException;
import org.knowm.xchange.instrument.Instrument;
import org.knowm.xchange.service.marketdata.MarketDataService;
import org.knowm.xchange.service.marketdata.params.Params;
import org.knowm.xchange.utils.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BybitMarketDataService extends BybitMarketDataServiceRaw implements MarketDataService {

  public BybitMarketDataService(BybitExchange  exchange, ResilienceRegistries resilienceRegistries) {
    super(exchange, resilienceRegistries);
  }

  @Override public Ticker getTicker(Instrument instrument, Object... args) throws IOException {
    Assert.notNull(instrument, "Null instrument");

    BybitCategory category = BybitAdapters.getCategory(instrument);

    BybitResult<BybitTickers<BybitTicker>> response = getTicker24h(category, BybitAdapters.convertToBybitSymbol(instrument));

    if (response.getResult().getList().isEmpty()) {
      return new Ticker.Builder().build();
    } else {
      BybitTicker bybitTicker = response.getResult().getList().get(0);

      switch (category) {
        case SPOT:
          return BybitAdapters.adaptBybitSpotTicker(instrument, response.getTime(), (BybitSpotTicker) bybitTicker);
        case LINEAR:
        case INVERSE:
          return BybitAdapters.adaptBybitLinearInverseTicker(instrument, response.getTime(), (BybitLinearInverseTicker) bybitTicker);
        case OPTION:
          return BybitAdapters.adaptBybitOptionTicker(instrument, response.getTime(), (BybitOptionTicker) bybitTicker);
        default:
          throw new IllegalStateException("Unexpected value: " + category);
      }
    }
  }

  @Override public Ticker getTicker(CurrencyPair currencyPair, Object... args) throws IOException {
    return getTicker((Instrument) currencyPair, args);
  }

  @Override
  public Trades getTrades(Instrument instrument, Object... args) throws IOException {
    Assert.notNull(instrument, "Null instrument");

    BybitCategory category = BybitAdapters.getCategory(instrument);

    BybitResult<BybitTradeResponse> response = getBybitTrades(category,
        BybitAdapters.convertToBybitSymbol(instrument));

    if (response.getResult() == null) {
      return null;
    } else {

      return BybitAdapters.adaptBybitTrades(response.getResult().getList(), instrument);

    }
  }

  @Override public OrderBook getOrderBook(Instrument instrument, Object... args) throws IOException {
    Assert.notNull(instrument, "Null instrument");

    BybitCategory category = BybitAdapters.getCategory(instrument);

    BybitResult<BybitOrderbook> response = getOrderBook(category, BybitAdapters.convertToBybitSymbol(instrument));

    if (response.getResult() == null) {
      return null;
    } else {

      return BybitAdapters.adaptBybitOrderBook(response.getResult(), instrument);
/*      switch (category) {
        case SPOT:
          return BybitAdapters.adaptBybitSpotTicker(
              instrument, response.getTime(), (BybitSpotTicker) bybitTicker);
        case LINEAR:
        case INVERSE:
          return BybitAdapters.adaptBybitLinearInverseTicker(
              instrument, response.getTime(), (BybitLinearInverseTicker) bybitTicker);
        case OPTION:
          return BybitAdapters.adaptBybitOptionTicker(
              instrument, response.getTime(), (BybitOptionTicker) bybitTicker);
        default:
          throw new IllegalStateException("Unexpected value: " + category);
      }*/
      // }

    }
  }



  @Override public List<Ticker> getTickers(Params params) throws IOException {
    if (!(params instanceof BybitCategory)) {
      throw new IllegalArgumentException("Params must be instance of BybitCategory");
    }
    BybitCategory category = (BybitCategory) params;
    if (category.equals(BybitCategory.OPTION)) {
      throw new NotYetImplementedForExchangeException("category OPTION not yet implemented");
    }
    BybitResult<BybitTickers<BybitTicker>> response = getTickers(category);
    List<Ticker> result = new ArrayList<>();
    for (BybitTicker ticker : response.getResult().getList()) {
      switch (category) {
        case SPOT:
          result.add(
              BybitAdapters.adaptBybitSpotTicker(BybitAdapters.convertBybitSymbolToInstrument(ticker.getSymbol(), category), response.getTime(),
                  (BybitSpotTicker) ticker));
          break;
        case LINEAR:
        case INVERSE:
          result.add(BybitAdapters.adaptBybitLinearInverseTicker(BybitAdapters.convertBybitSymbolToInstrument(ticker.getSymbol(), category),
              response.getTime(), (BybitLinearInverseTicker) ticker));
          break;
        default:
      }
    }
    return result;
  }

}

