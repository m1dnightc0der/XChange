package info.bitrich.xchangestream.hyperliquid.p2p;

import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingAccountService;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import info.bitrich.xchangestream.core.StreamingTradeService;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Observable;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.derivative.FuturesContract;
import org.knowm.xchange.hyperliquid.HyperliquidAdapters;
import org.knowm.xchange.hyperliquid.HyperliquidExchange;
import org.knowm.xchange.instrument.Instrument;

/** Explicit public-only sibling. The native REST/private exchange and its routing are unchanged. */
public class P2pHyperliquidStreamingExchange extends HyperliquidExchange implements StreamingExchange {
  public static final String P2P_WS_URI = "p2p.wsuri";
  public static final String P2P_SNAPSHOT_URI = "p2p.snapshoturi";
  public static final String P2P_SNAPSHOT_REDIRECT_ORIGINS = "p2p.snapshot.redirectorigins";
  private P2pStreamingMarketDataService marketData;
  private volatile P2pStreamingService service;
  private Map<Instrument, String> instruments = Collections.emptyMap();
  private Map<Instrument, String> tradeInstruments = Collections.emptyMap();
  private Completable connection;
  private boolean autoReconnect = true;
  private boolean connectionPending;
  private long connectionAttempt;

  @Override public void applySpecification(ExchangeSpecification specification) {
    if (service != null) throw new IllegalStateException("P2P specification is immutable after connect");
    rejectCredentials(specification);
    super.applySpecification(specification);
  }

  @Override protected void initServices() {
    rejectCredentials(exchangeSpecification);
    super.initServices(); // Retain native REST endpoints and metadata; never insert P2P URLs here.
    marketData = new P2pStreamingMarketDataService(this);
  }

  protected P2pStreamingService createP2pService(URI websocket, Set<String> coins,
      P2pSnapshotClient snapshots) {
    return new P2pStreamingService(websocket, coins, snapshots);
  }

  protected P2pStreamingService createP2pService(URI websocket, Set<String> bookCoins,
      Set<String> tradeCoins, P2pSnapshotClient snapshots) {
    return tradeCoins.isEmpty() ? createP2pService(websocket, bookCoins, snapshots)
        : new P2pStreamingService(websocket, bookCoins, tradeCoins, snapshots);
  }

  @Override public Completable connect(ProductSubscription... subscriptions) {
    return Completable.defer(() -> {
      synchronized (this) {
        rejectCredentials(exchangeSpecification);
        Membership membership = membership(subscriptions);
        if (service != null) {
          if (!instruments.equals(membership.instruments) || !tradeInstruments.equals(membership.trades)) {
            return Completable.error(new IllegalArgumentException("P2P membership is immutable"));
          }
          if (!autoReconnect) {
            if (service.getReadiness() == P2pStreamingService.Readiness.FAILED) {
              return Completable.error(service.getLastFailure());
            }
            if (service.isTerminal()) {
              return Completable.error(new IllegalStateException("Create a new exchange after explicit stop"));
            }
            if (connectionPending || service.getReadiness() == P2pStreamingService.Readiness.LIVE) {
              return connection;
            }
            return startConnectionAttempt(service);
          }
          if (service.getReadiness() == P2pStreamingService.Readiness.DISCONNECTED
              || service.getReadiness() == P2pStreamingService.Readiness.FAILED) {
            return Completable.error(new IllegalStateException("Create a new exchange after explicit stop"));
          }
          return service.awaitLive();
        }
        if (marketData == null) throw new IllegalStateException("Apply specification first");
        URI websocket = requiredUri(P2P_WS_URI);
        P2pSnapshotClient client = membership.coins.isEmpty() ? null
            : new P2pSnapshotClient(requiredUri(P2P_SNAPSHOT_URI), redirectOrigins(), new P2pWireCodec());
        P2pStreamingService created = createP2pService(
            websocket, membership.coins, membership.tradeCoins, client);
        autoReconnect = !Boolean.FALSE.equals(
            exchangeSpecification.getExchangeSpecificParametersItem(StreamingExchange.AUTO_RECONNECT));
        // Ownership must be established before publication or the first socket/snapshot attempt.
        created.setAutoReconnect(autoReconnect);
        instruments = membership.instruments;
        tradeInstruments = membership.trades;
        // Publish both immutable maps before making the service visible to subscribers.
        service = created;
        if (!autoReconnect) return startConnectionAttempt(created);
        connection = created.connect().andThen(Completable.defer(created::awaitLive))
            .doOnError(error -> disconnect().subscribe(() -> {}, ignored -> {})).cache();
        return connection;
      }
    });
  }

  /** Called under the exchange monitor; all callers of one pending hosted attempt join its cache. */
  private Completable startConnectionAttempt(P2pStreamingService current) {
    long attempt = ++connectionAttempt;
    connectionPending = true;
    connection = current.connect()
        .doOnComplete(() -> {
          synchronized (this) {
            if (connectionAttempt == attempt) connectionPending = false;
          }
        })
        .doOnError(error -> {
          synchronized (this) {
            if (connectionAttempt == attempt) {
              connectionPending = false;
              if (!current.isTerminal()) connection = null;
            }
          }
        }).cache();
    return connection;
  }

  @Override public Completable disconnect() {
    return service == null ? Completable.complete() : service.disconnect();
  }

  @Override public boolean isAlive() {
    return service != null && service.isSocketOpen();
  }

  public P2pStreamingService getP2pStreamingService() { return service; }
  public P2pStreamingService.Readiness getReadiness() {
    return service == null ? P2pStreamingService.Readiness.DISCONNECTED : service.getReadiness();
  }
  public long getGeneration() { return service == null ? 0 : service.getGeneration(); }
  public P2pStreamingService.Cursor getCursor() { return service == null ? null : service.getCursor(); }
  public Set<String> getSubscribedCoins() {
    return service == null ? Collections.emptySet() : service.getSubscribedCoins();
  }
  public Throwable getLastFailure() { return service == null ? null : service.getLastFailure(); }
  public Observable<P2pStreamingService.Observation> observations() {
    return Observable.defer(() -> requireService().observations());
  }
  public Observable<P2pStreamingService.Readiness> readiness() {
    return Observable.defer(() -> requireService().readiness());
  }

  @Override public StreamingMarketDataService getStreamingMarketDataService() { return marketData; }
  @Override public StreamingTradeService getStreamingTradeService() {
    throw new UnsupportedOperationException("P2P is public-only; use the separate native private exchange");
  }
  @Override public StreamingAccountService getStreamingAccountService() {
    throw new UnsupportedOperationException("P2P is public-only");
  }
  @Override public void useCompressedMessages(boolean compressed) {
    if (compressed) throw new UnsupportedOperationException("P2P binary messages are not compressed");
  }

  P2pStreamingService requireService() {
    P2pStreamingService current = service;
    if (current == null) throw new IllegalStateException("Connect with complete membership first");
    return current;
  }
  String coinFor(Instrument instrument) {
    String coin = instruments.get(instrument);
    if (coin == null) throw new IllegalArgumentException("Instrument is outside P2P membership");
    return coin;
  }
  void requireTrade(Instrument instrument) {
    if (!tradeInstruments.containsKey(instrument)) throw new IllegalArgumentException("Unsubscribed P2P trade instrument");
  }
  String tradeCoinFor(Instrument instrument) {
    requireTrade(instrument);
    return tradeInstruments.get(instrument);
  }

  private URI requiredUri(String key) {
    Object value = exchangeSpecification.getExchangeSpecificParametersItem(key);
    if (value == null || value.toString().trim().isEmpty()) throw new IllegalArgumentException("Missing " + key);
    return URI.create(value.toString());
  }

  private Set<URI> redirectOrigins() {
    Object configured = exchangeSpecification.getExchangeSpecificParametersItem(P2P_SNAPSHOT_REDIRECT_ORIGINS);
    Set<URI> result = new LinkedHashSet<>();
    if (configured instanceof Collection) {
      for (Object origin : (Collection<?>) configured) result.add(URI.create(String.valueOf(origin).trim()));
    } else if (configured != null && !configured.toString().trim().isEmpty()) {
      for (String origin : configured.toString().split(",")) result.add(URI.create(origin.trim()));
    }
    return result;
  }

  static void rejectCredentials(ExchangeSpecification spec) {
    if (spec == null) return;
    if (nonempty(spec.getApiKey()) || nonempty(spec.getSecretKey()) || nonempty(spec.getPassword())
        || nonempty(spec.getUserName())) throw new IllegalArgumentException("P2P rejects credentials");
    if (spec.getExchangeSpecificParameters() != null) {
      for (Map.Entry<String, Object> parameter : spec.getExchangeSpecificParameters().entrySet()) {
        String key = parameter.getKey().toLowerCase(java.util.Locale.ROOT).replace("_", "");
        if ((key.contains("wallet") || key.contains("vault") || key.contains("privatekey")
            || key.contains("secret") || key.contains("apikey") || key.contains("passphrase")
            || key.contains("password"))
            && parameter.getValue() != null && !parameter.getValue().toString().trim().isEmpty()) {
          throw new IllegalArgumentException("P2P rejects private parameters");
        }
      }
    }
  }
  private static boolean nonempty(String value) { return value != null && !value.trim().isEmpty(); }

  static Membership membership(ProductSubscription... subscriptions) {
    if (subscriptions == null || subscriptions.length == 0) throw new IllegalArgumentException("Missing products");
    Map<Instrument, String> instruments = new LinkedHashMap<>();
    Set<String> coins = new java.util.TreeSet<>();
    Map<Instrument, String> trades = new LinkedHashMap<>();
    Set<String> tradeCoins = new java.util.TreeSet<>();
    for (ProductSubscription subscription : subscriptions) {
      if (subscription == null || subscription.hasAuthenticated() || !subscription.getFundingRates().isEmpty()) {
        throw new IllegalArgumentException("Only public books, BBO and trades are supported");
      }
      Set<Instrument> union = new LinkedHashSet<>(subscription.getOrderBook());
      union.addAll(subscription.getTicker());
      for (Instrument instrument : union) {
        String coin = supportedCoin(instrument);
        instruments.put(instrument, coin);
        coins.add(coin);
      }
      for (Instrument instrument : subscription.getTrades()) {
        String coin = supportedCoin(instrument);
        trades.put(instrument, coin);
        tradeCoins.add(coin);
      }
    }
    Set<String> allCoins = new LinkedHashSet<>(coins);
    allCoins.addAll(tradeCoins);
    if (allCoins.isEmpty() || allCoins.size() > 512) throw new IllegalArgumentException("P2P requires 1..512 public coins");
    return new Membership(instruments, coins, trades, tradeCoins);
  }

  private static String supportedCoin(Instrument instrument) {
    if (!(instrument instanceof FuturesContract) || !((FuturesContract) instrument).isPerpetual()) {
      throw new IllegalArgumentException("P2P requires supported perpetual instruments");
    }
    String coin = HyperliquidAdapters.adaptInstrumentToCoin(instrument);
    if (coin == null || coin.isEmpty() || coin.contains(",") || coin.length() > 255) {
      throw new IllegalArgumentException("Invalid instrument mapping");
    }
    return coin;
  }

  static final class Membership {
    final Map<Instrument, String> instruments;
    final Set<String> coins;
    final Map<Instrument, String> trades;
    final Set<String> tradeCoins;
    Membership(Map<Instrument, String> instruments, Set<String> coins,
        Map<Instrument, String> trades, Set<String> tradeCoins) {
      this.instruments = Collections.unmodifiableMap(new LinkedHashMap<>(instruments));
      this.coins = Collections.unmodifiableSet(new LinkedHashSet<>(coins));
      this.trades = Collections.unmodifiableMap(new LinkedHashMap<>(trades));
      this.tradeCoins = Collections.unmodifiableSet(new LinkedHashSet<>(tradeCoins));
    }
  }
}
