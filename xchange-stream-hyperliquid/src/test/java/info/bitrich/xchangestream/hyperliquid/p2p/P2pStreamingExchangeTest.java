package info.bitrich.xchangestream.hyperliquid.p2p;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.hyperliquid.HyperliquidStreamingExchange;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.observers.TestObserver;
import io.reactivex.rxjava3.schedulers.TestScheduler;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.derivative.FuturesContract;
import org.knowm.xchange.dto.marketdata.Trade;

public class P2pStreamingExchangeTest {
  private static final FuturesContract BTC = new FuturesContract("BTC/USDC/PERP");
  private static final FuturesContract ETH = new FuturesContract("ETH/USDC/PERP");

  @Test public void immutableExactMembershipUsesCompleteUnionNotHardcodedMajors() {
    ProductSubscription.ProductSubscriptionBuilder books = ProductSubscription.create();
    ProductSubscription.ProductSubscriptionBuilder tickers = ProductSubscription.create();
    Set<String> expected = new HashSet<>();
    for (int i = 0; i < 50; i++) {
      FuturesContract instrument = new FuturesContract("COIN" + i + "/USDC/PERP");
      (i % 2 == 0 ? books : tickers).addOrderbook(instrument);
      tickers.addTicker(instrument);
      expected.add("COIN" + i);
    }
    P2pHyperliquidStreamingExchange.Membership membership = P2pHyperliquidStreamingExchange.membership(books.build(), tickers.build());
    assertEquals(expected, membership.coins);
    assertEquals(50, membership.instruments.size());
    assertThrows(UnsupportedOperationException.class, () -> membership.coins.add("EXTRA"));
    assertThrows(UnsupportedOperationException.class, () -> membership.instruments.clear());
  }

  @Test public void privateEmptySpotDatedAndOversizedSubscriptionsAreRejected() {
    assertThrows(IllegalArgumentException.class, () -> P2pHyperliquidStreamingExchange.membership());
    for (ProductSubscription invalid : new ProductSubscription[] {
        ProductSubscription.create().build(),
        ProductSubscription.create().addOrderbook(CurrencyPair.BTC_USD).build(),
        ProductSubscription.create().addOrderbook(new FuturesContract("BTC/USDC/202612" )).build(),
        ProductSubscription.create().addOrderbook(BTC).addOrders(BTC).build(),
        ProductSubscription.create().addOrderbook(BTC).addUserTrades(BTC).build(),
        ProductSubscription.create().addOrderbook(BTC).addFundingRates(BTC).build()}) {
      assertThrows(IllegalArgumentException.class, () -> P2pHyperliquidStreamingExchange.membership(invalid));
    }
    ProductSubscription.ProductSubscriptionBuilder tooMany = ProductSubscription.create();
    for (int i = 0; i < 513; i++) tooMany.addOrderbook(new FuturesContract("C" + i + "/USDC/PERP"));
    assertThrows(IllegalArgumentException.class, () -> P2pHyperliquidStreamingExchange.membership(tooMany.build()));
  }

  @Test public void credentialsAndPrivateServiceAccessAreRejectedBeforeInitialization() {
    for (String key : new String[] {"api", "secret", "wallet", "vault", "privateKey"}) {
      P2pHyperliquidStreamingExchange exchange = new P2pHyperliquidStreamingExchange();
      ExchangeSpecification spec = exchange.getDefaultExchangeSpecification();
      if (key.equals("api")) spec.setApiKey("not-a-real-key");
      else if (key.equals("secret")) spec.setSecretKey("not-a-real-key");
      else spec.setExchangeSpecificParametersItem(key, "not-a-real-key");
      assertThrows(IllegalArgumentException.class, () -> exchange.applySpecification(spec));
    }
    P2pHyperliquidStreamingExchange exchange = new P2pHyperliquidStreamingExchange();
    assertThrows(UnsupportedOperationException.class, exchange::getStreamingTradeService);
    assertThrows(UnsupportedOperationException.class, exchange::getStreamingAccountService);
  }

  @Test public void booksAndTradesUseOneP2pServiceAndKeepNativeRestConfiguration() {
    TestExchange exchange = configured(true);
    ProductSubscription products = ProductSubscription.create().addOrderbook(BTC).addTicker(ETH)
        .addTrades(BTC).build();
    exchange.connect(products).test().assertComplete();
    assertEquals(new HashSet<>(java.util.Arrays.asList("BTC", "ETH")), exchange.getSubscribedCoins());
    assertEquals(new HashSet<>(java.util.Arrays.asList("BTC", "ETH")), exchange.bookCoins);
    assertEquals(Collections.singleton("BTC"), exchange.tradeCoins);
    assertEquals("wss://p2p.invalid/ws", exchange.websocket.toString());
    assertEquals("wss://native-public.invalid/ws", exchange.getExchangeSpecification().getOverrideWebsocketApiUri());
    assertFalse(exchange.getExchangeSpecification().getSslUri().contains("p2p"));
    for (java.lang.reflect.Field field : P2pHyperliquidStreamingExchange.class.getDeclaredFields()) {
      assertFalse("No native public delegate", HyperliquidStreamingExchange.class.isAssignableFrom(field.getType()));
    }
    exchange.connect(products).test().assertComplete();
    verify(exchange.stub, times(1)).connect();
    exchange.connect(ProductSubscription.create().addOrderbook(BTC).build()).test()
        .assertError(IllegalArgumentException.class);
    exchange.getStreamingMarketDataService().getOrderBook(new FuturesContract("SOL/USDC/PERP"))
        .test().assertError(IllegalArgumentException.class);
    exchange.getStreamingMarketDataService().getTrades(ETH).test().assertError(IllegalArgumentException.class);
    exchange.disconnect().test().assertComplete();
    verify(exchange.stub).disconnect();
  }

  @Test public void tradeOnlyNeedsNoSnapshotAndSeparateMembershipIsEnforced() {
    TestExchange exchange = configured(false);
    exchange.connect(ProductSubscription.create().addTrades(ETH).build()).test().assertComplete();
    assertTrue(exchange.bookCoins.isEmpty());
    assertEquals(Collections.singleton("ETH"), exchange.tradeCoins);
    assertNull(exchange.snapshots);
    assertEquals(Collections.singleton("ETH"), exchange.getSubscribedCoins());
    exchange.getStreamingMarketDataService().getOrderBook(ETH).test().assertError(IllegalArgumentException.class);
    exchange.getStreamingMarketDataService().getTicker(ETH).test().assertError(IllegalArgumentException.class);
    exchange.getStreamingMarketDataService().getTrades(BTC).test().assertError(IllegalArgumentException.class);
    exchange.disconnect().test().assertComplete();
  }

  @Test public void p2pTradesAdaptExactSignedValuesEventTimeIdsAndObserverIsolation() throws Exception {
    TestExchange exchange = configured(false);
    exchange.connect(ProductSubscription.create().addTrades(BTC).build()).test().assertComplete();
    PublishSubject<P2pStreamingService.TradePublication> events = PublishSubject.create();
    when(exchange.stub.trades("BTC")).thenReturn(events);
    TestObserver<Trade> first = exchange.getStreamingMarketDataService().getTrades(BTC).test();
    TestObserver<Trade> second = exchange.getStreamingMarketDataService().getTrades(BTC).test();
    P2pWireCodec.TradeSide side = new P2pWireCodec.TradeSide("00", BigInteger.ONE, null, null, BigDecimal.ZERO);
    java.lang.reflect.Constructor<P2pStreamingService.TradePublication> publication =
        P2pStreamingService.TradePublication.class.getDeclaredConstructor(String.class, P2pWireCodec.TradeEvent.class);
    publication.setAccessible(true);
    for (int i = 0; i < 2; i++) {
      P2pWireCodec.TradeEvent event = new P2pWireCodec.TradeEvent(BigInteger.valueOf(1234567890),
          i == 0, null, "BTC", new BigDecimal("12.345678901"), new BigDecimal("0.00012345"),
          "Na", side, side);
      events.onNext(publication.newInstance("p2p:trades:101:1:" + i, event));
    }
    first.assertValueCount(2); second.assertValueCount(2);
    Trade buy = first.values().get(0), sell = first.values().get(1);
    assertEquals(org.knowm.xchange.dto.Order.OrderType.BID, buy.getType());
    assertEquals(org.knowm.xchange.dto.Order.OrderType.ASK, sell.getType());
    assertEquals(new BigDecimal("0.00012345"), buy.getOriginalAmount());
    assertEquals(new BigDecimal("-0.00012345"), sell.getOriginalAmount());
    assertEquals(new BigDecimal("12.345678901"), sell.getPrice());
    assertEquals(1234567L, buy.getTimestamp().getTime());
    assertEquals("p2p:trades:101:1:0", buy.getId());
    assertEquals("p2p:trades:101:1:1", sell.getId());
    assertEquals(BTC, sell.getInstrument());
    assertNotSame(buy, second.values().get(0));
    buy.getTimestamp().setTime(0);
    assertEquals(1234567L, second.values().get(0).getTimestamp().getTime());
    first.dispose();
    exchange.getStreamingMarketDataService().getTrades(BTC).test().assertNoValues();
    exchange.disconnect().test().assertComplete();
  }

  @Test public void tickerOnlyUsesBookMembershipAndCannotRequestTrades() {
    TestExchange exchange = configured(true);
    exchange.connect(ProductSubscription.create().addTicker(BTC).build()).test().assertComplete();
    assertEquals(Collections.singleton("BTC"), exchange.bookCoins);
    assertTrue(exchange.tradeCoins.isEmpty());
    assertNotNull(exchange.snapshots);
    when(exchange.stub.book("BTC")).thenReturn(io.reactivex.rxjava3.core.Observable.never());
    TestObserver<org.knowm.xchange.dto.marketdata.Ticker> ticker =
        exchange.getStreamingMarketDataService().getTicker(BTC).test();
    ticker.assertNoErrors().assertNoValues();
    exchange.getStreamingMarketDataService().getTrades(BTC).test()
        .assertError(IllegalArgumentException.class);
    verify(exchange.stub, never()).trades(anyString());
    ticker.dispose();
    exchange.disconnect().test().assertComplete();
  }

  @Test public void bookMembershipStillRequiresSnapshotEndpoint() {
    TestExchange exchange = configured(false);
    exchange.connect(ProductSubscription.create().addOrderbook(BTC).build()).test()
        .assertError(IllegalArgumentException.class);
    assertNull(exchange.stub);
  }

  @Test(timeout = 10000) public void connectPreservesTerminalCodeAndCauseBeforeTestDeadline() throws Exception {
    BoundaryExchange exchange = new BoundaryExchange();
    TestScheduler deadline = new TestScheduler();
    TestObserver<Void> connection = exchange.connect(boundaryProducts())
        .timeout(1, TimeUnit.SECONDS, deadline).test();
    try {
      negotiateBookReplay(exchange);
      connection.assertNotComplete().assertNoErrors();
      exchange.provider.text("{\"channel\":\"errors\",\"code\":\"version_mismatch\","
          + "\"message\":\"requested binary version is unsupported\"}");
      P2pStreamingServiceTest.waitFor(() -> exchange.getLastFailure() != null);
      connection.awaitDone(1, TimeUnit.SECONDS).assertNotComplete()
          .assertError(error -> error == exchange.getLastFailure()
              && error instanceof IOException
              && error.getMessage().contains("version_mismatch")
              && error.getMessage().contains("requested binary version is unsupported"));
      // The provider exception, not the deliberately short external deadline, terminates connect.
      deadline.advanceTimeBy(1, TimeUnit.SECONDS);
      connection.assertError(error -> error == exchange.getLastFailure());
      assertEquals(1, exchange.disconnectCalls.get());
      assertEquals(P2pStreamingService.Readiness.DISCONNECTED, exchange.getReadiness());
      assertEquals(1, exchange.provider.channelsCreated.size());
      assertFalse(exchange.provider.channel.isOpen());
    } finally {
      connection.dispose();
      exchange.provider.stop();
    }
  }

  @Test(timeout = 10000) public void connectSurvivesDocumentedHistoryErrorWithoutCleanupStoppingRecovery() throws Exception {
    BoundaryExchange exchange = new BoundaryExchange();
    TestObserver<Void> connection = exchange.connect(boundaryProducts()).test();
    TestObserver<Trade> trades = exchange.getStreamingMarketDataService().getTrades(ETH).test();
    try {
      negotiateBookReplay(exchange);
      long generation = exchange.getGeneration();
      exchange.provider.text("{\"channel\":\"errors\",\"code\":\"history_error\",\"message\":\"reader failed\"}");
      P2pStreamingServiceTest.waitFor(() -> exchange.getLastFailure() != null
          && !exchange.provider.channel.isOpen());
      connection.assertNotComplete().assertNoErrors();
      assertEquals(0, exchange.disconnectCalls.get());
      assertEquals(P2pStreamingService.Readiness.CATCHING_UP, exchange.getReadiness());
      assertTrue(exchange.getGeneration() > generation);
      assertTrue(exchange.getLastFailure() instanceof IOException);
      assertTrue(exchange.getLastFailure().getMessage().contains("history_error"));
      assertTrue(exchange.getLastFailure().getMessage().contains("reader failed"));
      assertEquals(BigInteger.valueOf(101), exchange.getCursor().getHeight());
      assertEquals(0, exchange.getCursor().getFramesAtHeight());

      // This offline harness replaces only transport establishment. Drive a real new provider
      // connection; inherited socket retry/close-1011 ownership belongs to P2pNettyReconnectTest.
      // Do not call exchange.connect again: its original readiness wait must survive recovery.
      int before = exchange.provider.sent.size();
      exchange.provider.start();
      P2pStreamingServiceTest.waitFor(() -> exchange.provider.sent.size() == before + 1);
      exchange.provider.negotiate(boundaryInfo(), 2);
      assertEquals(2, exchange.provider.channelsCreated.size());
      assertNotSame(exchange.provider.channelsCreated.get(0), exchange.provider.channel);
      assertReplaySubscription(exchange.provider.sent.get(before + 2), null, "BTC");
      assertEquals(1, exchange.snapshotFetches.get());
      connection.assertNotComplete().assertNoErrors();
      exchange.provider.frame(SyntheticWire.diff(1, 101,
          SyntheticWire.order(1, true, "BTC", "99", "2")));
      exchange.provider.frame(SyntheticWire.status(0, 1, 101, 0));
      P2pStreamingServiceTest.waitFor(() -> exchange.provider.sent.size() == before + 4);
      assertReplaySubscription(exchange.provider.sent.get(before + 3), "trades", "ETH");
      connection.assertNotComplete().assertNoErrors(); // Book LIVE alone is insufficient.
      SyntheticWire.Encoded side = SyntheticWire.tradeSide(new byte[20], BigInteger.ONE, null, null, "0");
      exchange.provider.frame(SyntheticWire.trades(BigInteger.ONE, BigInteger.ZERO, BigInteger.ZERO,
          BigInteger.valueOf(101), BigInteger.ZERO,
          SyntheticWire.trade(BigInteger.valueOf(123456789), true, null, "ETH", "10.25", "2.5",
              "Open", side, side)).bytes);
      exchange.provider.frame(SyntheticWire.status(3, 1, 101, 0));
      P2pStreamingServiceTest.waitFor(() -> exchange.getReadiness() == P2pStreamingService.Readiness.LIVE);
      connection.awaitDone(1, TimeUnit.SECONDS).assertComplete().assertNoErrors();
      trades.assertValueCount(1).assertNoErrors();
      assertEquals("p2p:trades:101:1:0", trades.values().get(0).getId());
      assertEquals(BigInteger.valueOf(101), exchange.getCursor().getHeight());
      assertEquals(1, exchange.getCursor().getFramesAtHeight());
      assertEquals(BigInteger.valueOf(101), exchange.provider.getTradeCursor().getHeight());
      assertEquals(1, exchange.provider.getTradeCursor().getFramesAtHeight());
      assertEquals(0, exchange.disconnectCalls.get());
      assertEquals(1, exchange.snapshotFetches.get());
      assertTrue(exchange.provider.channel.isOpen());
    } finally {
      trades.dispose();
      connection.dispose();
      exchange.provider.stop();
    }
  }

  @Test(timeout = 10000) public void silentBookReplayTimesOutOnlyAtExternalTestDeadline() throws Exception {
    BoundaryExchange exchange = new BoundaryExchange();
    TestScheduler deadline = new TestScheduler();
    TestObserver<Void> connection = exchange.connect(boundaryProducts())
        .timeout(1, TimeUnit.SECONDS, deadline).test();
    try {
      negotiateBookReplay(exchange);
      deadline.advanceTimeBy(999, TimeUnit.MILLISECONDS);
      connection.assertNotComplete().assertNoErrors();
      assertNull(exchange.getLastFailure());
      deadline.advanceTimeBy(1, TimeUnit.MILLISECONDS);
      connection.assertNotComplete().assertError(TimeoutException.class);
      assertNull(exchange.getLastFailure());
      assertEquals(0, exchange.disconnectCalls.get());
      assertEquals(P2pStreamingService.Readiness.CATCHING_UP, exchange.getReadiness());
      assertEquals(3, exchange.provider.sent.size()); // Still before any trade subscription.
      assertEquals(1, exchange.provider.channelsCreated.size());
      assertTrue(exchange.provider.channel.isOpen());
    } finally {
      connection.dispose();
      exchange.provider.stop();
    }
  }

  private static ProductSubscription boundaryProducts() {
    return ProductSubscription.create().addOrderbook(BTC).addTrades(ETH).build();
  }

  private static String boundaryInfo() {
    return "{\"channel\":\"info\",\"binary_version\":3,\"streams\":{\"book\":\"on\",\"trades\":\"on\"}}";
  }

  private static void negotiateBookReplay(BoundaryExchange exchange) throws Exception {
    P2pStreamingServiceTest.waitFor(() -> exchange.provider.sent.size() == 1);
    assertEquals("{\"method\":\"info\"}", exchange.provider.sent.get(0));
    exchange.provider.negotiate(boundaryInfo(), 2);
    assertEquals("{\"method\":\"esp\",\"version\":3}", exchange.provider.sent.get(1));
    assertReplaySubscription(exchange.provider.sent.get(2), null, "BTC");
    assertEquals(1, exchange.snapshotFetches.get());
    assertEquals(P2pStreamingService.Readiness.CATCHING_UP, exchange.getReadiness());
    assertNull(exchange.getLastFailure());
  }

  private static void assertReplaySubscription(String message, String stream, String coin) throws Exception {
    JsonNode command = new ObjectMapper().readTree(message);
    assertEquals("subscribe", command.path("method").asText());
    if (stream == null) assertFalse(command.has("stream"));
    else assertEquals(stream, command.path("stream").asText());
    assertEquals(1, command.path("coins").size());
    assertEquals(coin, command.path("coins").get(0).asText());
    assertTrue(command.path("from_height").isIntegralNumber());
    assertEquals(101, command.path("from_height").asLong());
  }

  private static final class BoundaryExchange extends P2pHyperliquidStreamingExchange {
    final AtomicInteger snapshotFetches = new AtomicInteger();
    final AtomicInteger disconnectCalls = new AtomicInteger();
    P2pStreamingServiceTest.Harness provider;

    BoundaryExchange() {
      ExchangeSpecification spec = getDefaultExchangeSpecification();
      spec.setShouldLoadRemoteMetaData(false);
      spec.setOverrideWebsocketApiUri("wss://native-public.invalid/ws");
      spec.setExchangeSpecificParametersItem(P2P_WS_URI, "wss://offline.invalid/ws");
      spec.setExchangeSpecificParametersItem(P2P_SNAPSHOT_URI, "https://snapshot.invalid/book");
      applySpecification(spec);
    }

    @Override protected P2pStreamingService createP2pService(URI websocket, Set<String> books,
        Set<String> trades, P2pSnapshotClient snapshots) {
      provider = new P2pStreamingServiceTest.Harness(books, trades, (coins, minimum) -> {
        assertEquals(Collections.singleton("BTC"), coins);
        assertEquals(BigInteger.ZERO, minimum);
        snapshotFetches.incrementAndGet();
        return SyntheticWire.seed(100, SyntheticWire.order(1, true, "BTC", "99", "1"),
            SyntheticWire.order(2, false, "BTC", "101", "1"));
      }, 32);
      return provider;
    }

    @Override public Completable disconnect() {
      disconnectCalls.incrementAndGet();
      return super.disconnect();
    }
  }

  private static TestExchange configured(boolean snapshot) {
    TestExchange exchange = new TestExchange();
    ExchangeSpecification spec = exchange.getDefaultExchangeSpecification();
    spec.setShouldLoadRemoteMetaData(false);
    spec.setOverrideWebsocketApiUri("wss://native-public.invalid/ws");
    spec.setExchangeSpecificParametersItem(P2pHyperliquidStreamingExchange.P2P_WS_URI, "wss://p2p.invalid/ws");
    if (snapshot) spec.setExchangeSpecificParametersItem(P2pHyperliquidStreamingExchange.P2P_SNAPSHOT_URI,
        "https://snapshot.invalid/book");
    exchange.applySpecification(spec);
    return exchange;
  }

  private static final class TestExchange extends P2pHyperliquidStreamingExchange {
    P2pStreamingService stub;
    URI websocket;
    Set<String> bookCoins, tradeCoins;
    P2pSnapshotClient snapshots;
    @Override protected P2pStreamingService createP2pService(URI websocket, Set<String> coins, P2pSnapshotClient snapshots) {
      return createP2pService(websocket, coins, Collections.emptySet(), snapshots);
    }
    @Override protected P2pStreamingService createP2pService(URI websocket, Set<String> books,
        Set<String> trades, P2pSnapshotClient snapshots) {
      this.websocket = websocket;
      this.bookCoins = books;
      this.tradeCoins = trades;
      this.snapshots = snapshots;
      Set<String> union = new HashSet<>(books);
      union.addAll(trades);
      stub = mock(P2pStreamingService.class);
      when(stub.getSubscribedCoins()).thenReturn(union);
      when(stub.getBookCoins()).thenReturn(books);
      when(stub.getTradeCoins()).thenReturn(trades);
      when(stub.getReadiness()).thenReturn(P2pStreamingService.Readiness.LIVE);
      when(stub.connect()).thenReturn(Completable.complete());
      when(stub.awaitLive()).thenReturn(Completable.complete());
      when(stub.disconnect()).thenReturn(Completable.complete());
      return stub;
    }
  }
}
