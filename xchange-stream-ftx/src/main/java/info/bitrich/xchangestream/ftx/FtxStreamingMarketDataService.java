package info.bitrich.xchangestream.ftx;

import com.google.common.collect.Lists;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import io.reactivex.rxjava3.core.Observable;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trade;
import org.knowm.xchange.ftx.FtxAdapters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

public class FtxStreamingMarketDataService implements StreamingMarketDataService {

  private static final Logger LOG = LoggerFactory.getLogger(FtxStreamingMarketDataService.class);

  private final FtxStreamingService service;

  public FtxStreamingMarketDataService(FtxStreamingService service) {
    this.service = service;
  }

  @Override
  public  Observable<OrderBook>  getOrderBook(CurrencyPair currencyPair, Object... args) {
    String channelName = "orderbook:" + FtxAdapters.adaptCurrencyPairToFtxMarket(currencyPair);
    FtxStreamingAdapters streamingAdapter = new FtxStreamingAdapters();
    AtomicReference<OrderBook> orderBook = new AtomicReference<>(new OrderBook(Date.from(Instant.now()), Lists.newArrayList(), Lists.newArrayList()));

    return service
        .subscribeChannel(channelName)
        .map(
            res -> {

              try {
                 return streamingAdapter.adaptOrderbookMessage(orderBook.get(), currencyPair, res);
              } catch (Exception e) {
                LOG.warn(
                    "Resubscribing {} channel after adapter error {}",
                    currencyPair,
                    e.getMessage());
                synchronized (orderBook.get().getBids()) {
                  orderBook.get().getBids().clear();
                }
                synchronized (orderBook.get().getAsks()) {
                  orderBook.get().getAsks().clear();
                }
                // Resubscribe to the channel
                this.service.sendMessage(service.getUnsubscribeMessage(channelName, args));
                this.service.sendMessage(service.getSubscribeMessage(channelName, args));
                orderBook.set(new OrderBook(Date.from(Instant.now()), Lists.newArrayList(), Lists.newArrayList(), false));
                return orderBook.get();
              }
            })
        .filter(ob -> ob.getBids().size() > 0 && ob.getAsks().size() > 0);
  }

  @Override
  public Observable<Ticker> getTicker(CurrencyPair currencyPair, Object... args) {
    FtxStreamingAdapters streamingAdapter = new FtxStreamingAdapters();

    return service
        .subscribeChannel("ticker:" + FtxAdapters.adaptCurrencyPairToFtxMarket(currencyPair))
        .map(res -> streamingAdapter.adaptTickerMessage(currencyPair, res))
        .filter(ticker -> ticker != streamingAdapter.NULL_TICKER); // lets not send these backs
  }

  @Override
  public Observable<Trade> getTrades(CurrencyPair currencyPair, Object... args) {
    FtxStreamingAdapters streamingAdapter = new FtxStreamingAdapters();

    return service
        .subscribeChannel("trades:" + FtxAdapters.adaptCurrencyPairToFtxMarket(currencyPair))
        .flatMapIterable(res -> streamingAdapter.adaptTradesMessage(currencyPair, res));
  }
}
