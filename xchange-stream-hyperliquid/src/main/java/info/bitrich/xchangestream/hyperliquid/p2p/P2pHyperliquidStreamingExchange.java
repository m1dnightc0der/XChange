package info.bitrich.xchangestream.hyperliquid.p2p;

import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingAccountService;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import info.bitrich.xchangestream.core.StreamingTradeService;
import info.bitrich.xchangestream.hyperliquid.HyperliquidStreamingExchange;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Observable;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
  private HyperliquidStreamingExchange nativePublic;
  private P2pStreamingMarketDataService marketData;
  private volatile P2pStreamingService service;
  private Map<Instrument, String> instruments = Collections.emptyMap();
  private Set<Instrument> tradeInstruments = Collections.emptySet();
  private Completable connection;

  @Override public void applySpecification(ExchangeSpecification specification) {
    if (service != null) throw new IllegalStateException("P2P specification is immutable after connect");
    rejectCredentials(specification);
    super.applySpecification(specification);
  }

  @Override protected void initServices() {
    rejectCredentials(exchangeSpecification);
    super.initServices(); // Retain native REST endpoints and metadata; never insert P2P URLs here.
    nativePublic = createPublicDelegate();
    ExchangeSpecification publicSpec = nativePublic.getDefaultExchangeSpecification();
    publicSpec.setShouldLoadRemoteMetaData(false);
    publicSpec.setSslUri(exchangeSpecification.getSslUri());
    publicSpec.setHost(exchangeSpecification.getHost());
    publicSpec.setOverrideWebsocketApiUri(exchangeSpecification.getOverrideWebsocketApiUri());
    publicSpec.setMetaDataJsonFileOverride(exchangeSpecification.getMetaDataJsonFileOverride());
    publicSpec.setExchangeSpecificParametersItem(HyperliquidExchange.USE_SANDBOX,
        exchangeSpecification.getExchangeSpecificParametersItem(HyperliquidExchange.USE_SANDBOX));
    nativePublic.applySpecification(publicSpec);
    marketData = new P2pStreamingMarketDataService(this);
  }

  protected HyperliquidStreamingExchange createPublicDelegate() {
    return new HyperliquidStreamingExchange();
  }

  protected P2pStreamingService createP2pService(URI websocket, Set<String> coins,
      P2pSnapshotClient snapshots) {
    return new P2pStreamingService(websocket, coins, snapshots);
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
          if (service.getReadiness() == P2pStreamingService.Readiness.DISCONNECTED
              || service.getReadiness() == P2pStreamingService.Readiness.FAILED) {
            return Completable.error(new IllegalStateException("Create a new exchange after explicit stop"));
          }
          return service.awaitLive();
        }
        if (nativePublic == null) throw new IllegalStateException("Apply specification first");
        URI websocket = requiredUri(P2P_WS_URI);
        URI snapshot = requiredUri(P2P_SNAPSHOT_URI);
        P2pSnapshotClient client = new P2pSnapshotClient(snapshot, redirectOrigins(), new P2pWireCodec());
        service = createP2pService(websocket, membership.coins, client);
        instruments = membership.instruments;
        tradeInstruments = membership.trades;
        ProductSubscription.ProductSubscriptionBuilder trades = ProductSubscription.create();
        tradeInstruments.forEach(trades::addTrades);
        Completable publicConnection = tradeInstruments.isEmpty() ? Completable.complete()
            : nativePublic.connect(trades.build());
        // Start public trades and P2P independently; only P2P readiness controls book usability.
        connection = Completable.mergeArray(publicConnection, service.connect().andThen(service.awaitLive()))
            .doOnError(error -> disconnect().subscribe(() -> {}, ignored -> {})).cache();
        return connection;
      }
    });
  }

  @Override public Completable disconnect() {
    Completable p2p = service == null ? Completable.complete() : service.disconnect();
    Completable nativeStop = nativePublic == null ? Completable.complete() : nativePublic.disconnect();
    return Completable.mergeArrayDelayError(p2p, nativeStop);
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
    if (!tradeInstruments.contains(instrument)) throw new IllegalArgumentException("Unsubscribed native trade instrument");
  }
  HyperliquidStreamingExchange publicDelegate() { return nativePublic; }

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
    Set<Instrument> trades = new LinkedHashSet<>();
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
        supportedCoin(instrument);
        trades.add(instrument);
      }
    }
    if (coins.isEmpty() || coins.size() > 512) throw new IllegalArgumentException("P2P requires 1..512 book/BBO coins");
    return new Membership(instruments, coins, trades);
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
    final Set<Instrument> trades;
    Membership(Map<Instrument, String> instruments, Set<String> coins, Set<Instrument> trades) {
      this.instruments = Collections.unmodifiableMap(new LinkedHashMap<>(instruments));
      this.coins = Collections.unmodifiableSet(new LinkedHashSet<>(coins));
      this.trades = Collections.unmodifiableSet(new LinkedHashSet<>(trades));
    }
  }
}
