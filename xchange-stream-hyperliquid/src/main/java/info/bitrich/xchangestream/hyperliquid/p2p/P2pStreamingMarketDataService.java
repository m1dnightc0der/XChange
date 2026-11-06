package info.bitrich.xchangestream.hyperliquid.p2p;

import info.bitrich.xchangestream.core.StreamingMarketDataService;
import io.reactivex.rxjava3.core.Observable;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trade;
import org.knowm.xchange.instrument.Instrument;

/** P2P full-depth books/BBO only; native credential-free public streaming supplies trades. */
public final class P2pStreamingMarketDataService implements StreamingMarketDataService {
  private final P2pHyperliquidStreamingExchange exchange;

  P2pStreamingMarketDataService(P2pHyperliquidStreamingExchange exchange) {
    this.exchange = exchange;
  }

  @Override public Observable<OrderBook> getOrderBook(Instrument instrument, Object... args) {
    return Observable.defer(() -> exchange.requireService().book(exchange.coinFor(instrument))
        .map(view -> view.toOrderBook(instrument)));
  }

  @Override public Observable<Ticker> getTicker(Instrument instrument, Object... args) {
    return Observable.defer(() -> exchange.requireService().book(exchange.coinFor(instrument))
        .map(view -> view.toTicker(instrument)));
  }

  @Override public Observable<Trade> getTrades(Instrument instrument, Object... args) {
    return Observable.defer(() -> {
      exchange.requireTrade(instrument);
      return exchange.publicDelegate().getStreamingMarketDataService().getTrades(instrument, args);
    });
  }
}
