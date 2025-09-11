package org.knowm.xchange.examples.deribit.dto.marketdata;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import org.knowm.xchange.Exchange;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.deribit.v2.dto.Kind;
import org.knowm.xchange.deribit.v2.dto.marketdata.DeribitCurrency;
import org.knowm.xchange.deribit.v2.dto.marketdata.DeribitInstrument;
import org.knowm.xchange.deribit.v2.dto.marketdata.DeribitOrderBook;
import org.knowm.xchange.deribit.v2.dto.marketdata.DeribitSummary;
import org.knowm.xchange.deribit.v2.dto.marketdata.DeribitTicker;
import org.knowm.xchange.deribit.v2.dto.marketdata.DeribitTrades;
import org.knowm.xchange.deribit.v2.service.DeribitMarketDataService;
import org.knowm.xchange.derivative.FuturesContract;
import org.knowm.xchange.derivative.OptionsContract;
import org.knowm.xchange.dto.marketdata.CandleStickData;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trades;
import org.knowm.xchange.examples.deribit.DeribitDemoUtils;
import org.knowm.xchange.okex.dto.trade.OkexTradeParams;
import org.knowm.xchange.service.marketdata.MarketDataService;
import org.knowm.xchange.service.trade.params.CandleStickDataParams;
import org.knowm.xchange.service.trade.params.DefaultCandleStickParam;
import java.util.Date;
public class DeribitMarketdataDemo {

  public static void main(String[] args) throws IOException {

    Exchange exchange = DeribitDemoUtils.createExchange();
    generic(exchange);
    raw(exchange);
  }

  private static void generic(Exchange exchange) throws IOException {
    MarketDataService genericService = exchange.getMarketDataService();

    CurrencyPair pair = new CurrencyPair("BTC", "PERPETUAL");

    Date start = Date.from( Instant.now().minus( Duration.ofDays( 720 ) ) );
    Date end=Date.from( Instant.now());
    CandleStickDataParams req =
            new DefaultCandleStickParam(start,end,300);
    Date expiryDate = Date.from(LocalDate.of(2025, 8, 1).atTime(8, 0, 0).toInstant(ZoneOffset.UTC));
    //FuturesContract contract = new FuturesContract(CurrencyPair.BTC_USDT, "PERPETUAL");
    OptionsContract contract = new OptionsContract("BTC/USD/250815/100000/P");

    CandleStickData candles = genericService.getCandleStickData(contract,req);
    System.out.println(candles);


    Ticker ticker = genericService.getTicker(pair);
    System.out.println(ticker);

    OrderBook orderBook = genericService.getOrderBook(pair);
    System.out.println(orderBook);

    Trades trades = genericService.getTrades(pair);
    System.out.println(trades);
  }

  private static void raw(Exchange exchange) throws IOException {
    DeribitMarketDataService service = (DeribitMarketDataService) exchange.getMarketDataService();

    String instrumentName = "BTC-PERPETUAL";
    String currency = "BTC";

    DeribitTicker ticker = service.getDeribitTicker(instrumentName);
    System.out.println(ticker);

    DeribitOrderBook orderBook = service.getDeribitOrderBook(instrumentName, null);
    System.out.println(orderBook);

    DeribitTrades trades =
        service.getLastTradesByInstrument(instrumentName, null, null, null, null, null);
    System.out.println(trades);

    List<DeribitCurrency> currencies = service.getDeribitCurrencies();
    System.out.println(currencies);

    List<DeribitInstrument> instruments =
        service.getDeribitInstruments(currency, Kind.future, false);
    System.out.println(instruments);

    List<DeribitSummary> summaries = service.getSummaryByInstrument(instrumentName);
    System.out.println(summaries);


  }
}
