package info.bitrich.xchangestream.hyperliquid.p2p;

import info.bitrich.xchangestream.core.StreamingMarketDataService;
import io.reactivex.rxjava3.core.Observable;
import java.math.BigInteger;
import java.util.Date;
import org.knowm.xchange.dto.Order.OrderType;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trade;
import org.knowm.xchange.instrument.Instrument;

/** Public P2P full-depth books, BBO and trades with observer-owned DTOs. */
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
    return Observable.defer(() -> exchange.requireService().trades(exchange.tradeCoinFor(instrument))
        .map(publication -> {
          P2pWireCodec.TradeEvent event = publication.event;
          return new Trade.Builder()
              .type(event.buy ? OrderType.BID : OrderType.ASK)
              .originalAmount(event.buy ? event.size.abs() : event.size.abs().negate())
              .instrument(instrument)
              .price(event.price)
              .timestamp(new Date(event.eventTimeMicros.divide(BigInteger.valueOf(1000)).longValueExact()))
              .id(publication.id)
              .build();
        }));
  }
}
