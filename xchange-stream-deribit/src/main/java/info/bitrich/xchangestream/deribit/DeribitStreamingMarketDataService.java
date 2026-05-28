package info.bitrich.xchangestream.deribit;

import static info.bitrich.xchangestream.deribit.DeribitStreamingService.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import info.bitrich.xchangestream.service.netty.NettyStreamingService;
import info.bitrich.xchangestream.service.netty.StreamingObjectMapperHelper;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.PublishSubject;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.deribit.v2.dto.marketdata.*;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.marketdata.*;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.exceptions.ExchangeException;
import org.knowm.xchange.exceptions.NotYetImplementedForExchangeException;
import org.knowm.xchange.instrument.Instrument;
import org.knowm.xchange.deribit.v2.DeribitAdapters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeribitStreamingMarketDataService implements StreamingMarketDataService {

  private static final Logger LOG = LoggerFactory.getLogger(DeribitStreamingMarketDataService.class);

  private final DeribitStreamingService service;

  private final ObjectMapper mapper = StreamingObjectMapperHelper.getObjectMapper();
  private final Map<Instrument, PublishSubject<List<OrderBookUpdate>>>
      orderBookUpdatesSubscriptions;

  public DeribitStreamingMarketDataService(DeribitStreamingService service) {
    this.service = service;
    this.orderBookUpdatesSubscriptions = new ConcurrentHashMap<>();
  }

  private final Map<String, OrderBook> orderBookMap = new HashMap<>();


  @Override
  public Observable<Trade> getTrades(Instrument instrument, Object... args) {
    String instId = DeribitAdapters.adaptInstrumentName(instrument).toString();
    String channelName=TRADES+"."+instId;
    if(args.length==0){
      channelName = channelName + ".100ms";
    } else {
      for (int i = 0; i < args.length; i++) {
        channelName = channelName + "." + args[i].toString();

      }
    }


    return service
        .subscribeChannel(channelName)
            .filter(message -> message.has("params"))
            .filter(message -> message.get("params").has("data"))
        .flatMap(
            jsonNode -> {
              List<DeribitTrade> deribitTradeList =
                  mapper.treeToValue(
                          jsonNode.get("params").get("data"),
                      mapper.getTypeFactory().constructCollectionType(List.class, DeribitTrade.class));
              return Observable.fromIterable(
                  DeribitAdapters.adaptTrades(deribitTradeList, instrument).getTrades());
            });
  }



  @Override
  public Observable<OrderBook> getOrderBook(Instrument instrument, Object... args) {
    AtomicLong orderBookPrevChangeId = new AtomicLong();

    String instId = DeribitAdapters.adaptInstrumentName(instrument).toString();

    String channelName=ORDERBOOK+"."+instId;
    if(args.length==0){
      channelName = channelName + ".100ms";
    } else {
      for (int i = 0; i < args.length; i++) {
        channelName = channelName + "." + args[i].toString();

      }
    }


String channelUniqueId=channelName;


    return service
        .subscribeChannel(channelName)
        .filter(message -> message.has("params"))
        .filter(message -> message.get("params").has("data"))
        .flatMap(
            jsonNode -> {
              // "books5" channel pushes 5 depth levels every time.
              String action = jsonNode.get("params").get("data").get("type").asText();
              DeribitStreamingOrderBook deribitOrderbooks =
                  mapper.treeToValue(
                      jsonNode.get("params").get("data"),
                      mapper
                          .getTypeFactory().constructType(DeribitStreamingOrderBook.class));
                 if ("snapshot".equalsIgnoreCase(action)) {
                OrderBook orderBook = DeribitAdapters.adaptOrderBook(deribitOrderbooks);
                orderBookPrevChangeId.set(deribitOrderbooks.getChangeId());
                orderBookMap.put(instId, orderBook);
                return Observable.just(orderBook);
              } else if ("change".equalsIgnoreCase(action)) {
                return applyChange(channelUniqueId, instrument, deribitOrderbooks, orderBookPrevChangeId);
              }
              return Observable.fromIterable(new LinkedList<>());

            });
  }


  @Override
  public Observable<Ticker> getTicker(Instrument instrument, Object... args) {
    String instId = DeribitAdapters.adaptInstrumentName(instrument).toString();
    String channelName=TICKER+"."+instId;
    if(args.length==0){
      channelName = channelName + ".100ms";
    } else {
      for (int i = 0; i < args.length; i++) {
        channelName = channelName + "." + args[i].toString();

      }
    }


    return service
            .subscribeChannel(channelName)
            .filter(message -> message.has("params"))
            .filter(message -> message.get("params").has("data"))
            .flatMap(
                    jsonNode -> {
                      DeribitTicker deribitTicker =
                              mapper.treeToValue(
                                      jsonNode.get("params").get("data"),
                                      mapper
                                              .getTypeFactory().constructType( DeribitTicker.class));
                      if (deribitTicker != null && "delivered".equalsIgnoreCase(deribitTicker.getState())) {
                        return Observable.error(new ExchangeException("terminal instrument state delivered for " + instId));
                      }
                      return Observable.just(
                              DeribitAdapters.adaptTicker(deribitTicker));
                    });
  }



  private Observable<OrderBook> applyChange(String channelUniqueId, Instrument instrument,
      DeribitStreamingOrderBook DeribitOrderBookUpdate,AtomicLong orderBookPrevChangeId) {
    String instId = DeribitAdapters.adaptInstrumentName(instrument).toString();
    OrderBook orderBook = orderBookMap.getOrDefault(instId, null);
    if (orderBook == null) {
      LOG.error("Failed to get orderBook, channelUniqueId= {}", channelUniqueId);
      return Observable.fromIterable(new LinkedList<>());
    }
   // LOG.error("Deribit instrument={},orderBookPrevChangeId={},ChangeId={},PrevChangeId={}",instrument, orderBookPrevChangeId.get(),DeribitOrderBookUpdate.getChangeId(),DeribitOrderBookUpdate.getPrevChangeId());
    if (orderBookPrevChangeId.get() == DeribitOrderBookUpdate.getPrevChangeId()) {
      orderBookPrevChangeId.set(DeribitOrderBookUpdate.getChangeId());
      LOG.debug("orderBookUpdate id {} ", DeribitOrderBookUpdate.getChangeId());
      TreeMap<BigDecimal, BigDecimal> asks = DeribitOrderBookUpdate.getAsks();
      TreeMap<BigDecimal, BigDecimal> bids = DeribitOrderBookUpdate.getBids();
      Date timestamp = DeribitOrderBookUpdate.getTimestamp();

      asks.keySet().forEach(
          derbitPrice ->
              orderBook.update(
                  new LimitOrder(Order.OrderType.ASK, asks.get(derbitPrice), instrument, null, timestamp, derbitPrice)));
      bids.keySet().forEach(
          derbitPrice ->
              orderBook.update(
                  new LimitOrder(Order.OrderType.BID, bids.get(derbitPrice), instrument, null, timestamp, derbitPrice)));




      if (orderBookUpdatesSubscriptions.get(instrument) != null) {
        orderBookUpdatesSubscriptions(instrument, asks, bids, timestamp);
      }
      return Observable.just(orderBook);
    } else if (orderBookPrevChangeId.get() !=0) {
      LOG.error("orderBookUpdate id sequence failed, expected {}, in fact {}",
          orderBookPrevChangeId,
          DeribitOrderBookUpdate.getChangeId());
      // resubscribe or what here?
      orderBookPrevChangeId.set(0);
          orderBookMap.remove(instrument);
      try {
        //service.getUnsubscribeMessage(channelUniqueId);
        service.sendMessage(service.getUnsubscribeMessage(channelUniqueId)).sync();
        service.resubscribeChannel(channelUniqueId);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      return Observable.fromIterable(new LinkedList<>());
    } else {
      return Observable.fromIterable(new LinkedList<>());
    }
  }


  @Override
  public Observable<List<OrderBookUpdate>> getOrderBookUpdates(Instrument instrument,
      Object... args) {
    return orderBookUpdatesSubscriptions.computeIfAbsent(instrument, v -> PublishSubject.create());
  }

  private void orderBookUpdatesSubscriptions(
      Instrument instrument, TreeMap<BigDecimal, BigDecimal> asks, TreeMap<BigDecimal, BigDecimal> bids, Date date) {
    List<OrderBookUpdate> orderBookUpdates = new ArrayList<>();
    for (BigDecimal ask : asks.keySet()) {
      OrderBookUpdate o =
          new OrderBookUpdate(
              Order.OrderType.ASK,
              asks.get(ask),
              instrument,
              ask,
              date,
              asks.get(ask));
      orderBookUpdates.add(o);
    }
    for (BigDecimal bid : bids.keySet()) {
      OrderBookUpdate o =
          new OrderBookUpdate(
              Order.OrderType.BID,
              bids.get(bid),
              instrument,
              bid,
              date,
              bids.get(bid));
      orderBookUpdates.add(o);
    }
    orderBookUpdatesSubscriptions.get(instrument).onNext(orderBookUpdates);
  }

}

