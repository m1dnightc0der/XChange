package info.bitrich.xchangestream.hyperliquid.p2p;

import static org.junit.Assert.*;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.hyperliquid.HyperliquidStreamingExchange;
import info.bitrich.xchangestream.service.netty.NettyStreamingService;
import info.bitrich.xchangestream.service.netty.WebSocketClientHandler;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.observers.TestObserver;
import io.reactivex.rxjava3.schedulers.TestScheduler;
import io.reactivex.rxjava3.subjects.PublishSubject;
import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.derivative.FuturesContract;
import org.knowm.xchange.dto.marketdata.OrderBook;
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

  @Test(timeout = 10000) public void autoReconnectFalseIsAppliedBeforeFirstSocketAttempt() throws Exception {
    assertAutoReconnectBeforeFirstSocketAttempt(Boolean.FALSE, false);
  }

  @Test(timeout = 10000) public void absentAutoReconnectKeepsStandaloneDefaultBeforeFirstSocketAttempt() throws Exception {
    assertAutoReconnectBeforeFirstSocketAttempt(null, true);
  }

  @Test(timeout = 10000) public void autoReconnectTrueKeepsStandaloneDefaultBeforeFirstSocketAttempt() throws Exception {
    assertAutoReconnectBeforeFirstSocketAttempt(Boolean.TRUE, true);
  }

  @Test(timeout = 10000) public void hostManagedPendingHandshakeFailurePreservesOriginalCauseBeforeChannelClose() throws Exception {
    SocketFailureExchange exchange = new SocketFailureExchange(null, true);
    AtomicReference<Throwable> connectError = new AtomicReference<>();
    TestObserver<Void> connection = exchange.connect(boundaryProducts()).doOnError(connectError::set).test();
    try {
      Field worker = P2pStreamingService.class.getDeclaredField("work");
      worker.setAccessible(true);
      ThreadPoolExecutor stateWorker = (ThreadPoolExecutor) worker.get(exchange.provider);
      P2pStreamingServiceTest.waitFor(() -> {
        SocketFailureService provider = exchange.provider;
        provider.pump();
        Throwable setupFailure = connectError.get();
        if (setupFailure == null) setupFailure = exchange.getLastFailure();
        if (setupFailure != null) {
          throw new AssertionError("Inherited Netty setup failed before handshake exception injection", setupFailure);
        }
        ChannelFuture transport = provider.handshakeTransport;
        if (transport != null && transport.isDone() && !transport.isSuccess()) {
          throw new AssertionError("Embedded transport connect failed before handshake exception injection", transport.cause());
        }
        if (provider.channel != null) {
          assertTrue("Embedded channel closed before handshake exception injection", provider.channel.isOpen());
        }
        // Let the preparation callback return: the inherited close/handshake listeners are installed.
        return transport != null && transport.isSuccess() && stateWorker.getActiveCount() == 0;
      });
      SocketFailureService provider = exchange.provider;
      assertEquals(1, provider.socketAttempts.get());
      assertEquals(1, provider.registrationAttempts.get());
      assertSame(provider.channel, inheritedSocketField("webSocketChannel").get(provider));
      assertSame(provider.handshakeHandler, provider.channel.pipeline().get(WebSocketClientHandler.class));
      assertTrue(provider.channel.isOpen());
      assertFalse(provider.channel.closeFuture().isDone());
      assertFalse(provider.handshakeHandler.handshakeFuture().isDone());
      assertTrue(provider.sent.isEmpty()); // No P2P info/replay: the inherited upgrade is still pending.
      connection.assertNotComplete().assertNoErrors();
      ByteBuf upgrade = provider.channel.readOutbound();
      assertNotNull(upgrade);
      try {
        assertTrue(upgrade.toString(StandardCharsets.US_ASCII).startsWith("GET /ws HTTP/1.1"));
      } finally {
        upgrade.release();
      }

      IOException original = new IOException("distinctive P2P failure during pending WebSocket upgrade");
      provider.channel.pipeline().fireExceptionCaught(original);
      provider.pump();
      assertSame(original, exchange.getLastFailure());
      assertNoInheritedSocketRetry(provider);
      connection.awaitDone(1, TimeUnit.SECONDS).assertNotComplete().assertError(error -> error == original);
      assertEquals(1, provider.socketAttempts.get());
    } finally {
      connection.dispose();
      if (exchange.provider != null) exchange.provider.stop();
    }
  }

  @Test(timeout = 10000) public void hostManagedInitialSocketFailureIsOneAttemptAndOrdinaryConnectRetries() throws Exception {
    IOException original = new IOException("initial socket registration failed");
    SocketFailureExchange exchange = new SocketFailureExchange(original);
    TestObserver<Void> first = exchange.connect(boundaryProducts()).test();
    TestObserver<Void> second = null;
    try {
      first.awaitDone(1, TimeUnit.SECONDS).assertNotComplete().assertError(error -> error == original);
      SocketFailureService provider = exchange.provider;
      assertEquals(1, provider.socketAttempts.get());
      assertEquals(1, provider.registrationAttempts.get()); // Actual inherited openConnection path.
      assertTrue(provider.channelsCreated.isEmpty());
      assertSame(original, exchange.getLastFailure());
      assertNoInheritedSocketRetry(provider);

      second = exchange.connect(boundaryProducts()).test();
      assertSame(provider, exchange.provider);
      assertEquals(1, exchange.serviceCreations.get());
      assertSocketMembership(provider);
      negotiateSocketAttempt(exchange, 0);
      second.assertNotComplete().assertNoErrors();
      finishSocketAttempt(exchange, second, 0);
      assertEquals(2, provider.socketAttempts.get());
      assertEquals(1, provider.registrationAttempts.get());
      assertEquals(1, provider.channelsCreated.size());
      assertNoInheritedSocketRetry(provider);
      first.assertError(error -> error == original);
    } finally {
      first.dispose();
      if (second != null) second.dispose();
      if (exchange.provider != null) exchange.provider.stop();
    }
  }

  @Test(timeout = 10000) public void hostManagedCatchingUpSocketErrorFailsConnectAndOrdinaryConnectRetries() throws Exception {
    IOException original = new IOException("socket failed while replay was catching up");
    SocketFailureExchange exchange = new SocketFailureExchange(null);
    TestObserver<Void> first = exchange.connect(boundaryProducts()).test();
    TestObserver<Void> second = null;
    try {
      negotiateSocketAttempt(exchange, 0);
      SocketFailureService provider = exchange.provider;
      EmbeddedChannel failedChannel = provider.channel;
      first.assertNotComplete().assertNoErrors();
      assertSame(failedChannel, inheritedSocketField("webSocketChannel").get(provider));
      failedChannel.pipeline().fireExceptionCaught(original);
      P2pStreamingServiceTest.waitFor(() -> {
        provider.pump();
        return !failedChannel.isOpen();
      });
      provider.pump();
      first.awaitDone(1, TimeUnit.SECONDS).assertNotComplete().assertError(error -> error == original);
      assertSame(original, exchange.getLastFailure());
      assertEquals(1, provider.socketAttempts.get());
      assertNoInheritedSocketRetry(provider);

      int before = provider.sent.size();
      second = exchange.connect(boundaryProducts()).test();
      assertSame(provider, exchange.provider);
      assertEquals(1, exchange.serviceCreations.get());
      assertSocketMembership(provider);
      negotiateSocketAttempt(exchange, before);
      assertNotSame(failedChannel, provider.channel);
      second.assertNotComplete().assertNoErrors();
      finishSocketAttempt(exchange, second, before);
      assertEquals(2, provider.socketAttempts.get());
      assertEquals(2, provider.channelsCreated.size());
      assertNoInheritedSocketRetry(provider);
      first.assertError(error -> error == original);
    } finally {
      first.dispose();
      if (second != null) second.dispose();
      if (exchange.provider != null) exchange.provider.stop();
    }
  }

  @Test(timeout = 10000) public void hostManagedLiveFailureErrorsBothStreamsAndFreshObservablesDeliver() throws Exception {
    SocketFailureExchange exchange = new SocketFailureExchange(null);
    TestObserver<Void> first = exchange.connect(boundaryProducts()).test();
    io.reactivex.rxjava3.core.Observable<org.knowm.xchange.dto.marketdata.OrderBook> books =
        exchange.getStreamingMarketDataService().getOrderBook(BTC);
    io.reactivex.rxjava3.core.Observable<Trade> trades = exchange.getStreamingMarketDataService().getTrades(ETH);
    TestObserver<org.knowm.xchange.dto.marketdata.OrderBook> oldBooks = books.test();
    TestObserver<Trade> oldTrades = trades.test();
    TestObserver<org.knowm.xchange.dto.marketdata.OrderBook> newBooks = null;
    TestObserver<Trade> newTrades = null;
    TestObserver<Void> second = null;
    try {
      negotiateSocketAttempt(exchange, 0);
      finishSocketAttempt(exchange, first, 0);
      oldBooks.assertValueCount(1).assertNoErrors();
      oldTrades.assertValueCount(1).assertNoErrors();
      SocketFailureService provider = exchange.provider;
      IOException original = new IOException("live public socket failed");
      provider.channel.pipeline().fireExceptionCaught(original);
      provider.pump();
      oldBooks.awaitDone(1, TimeUnit.SECONDS).assertError(error -> error == original);
      oldTrades.awaitDone(1, TimeUnit.SECONDS).assertError(error -> error == original);
      assertSame(original, exchange.getLastFailure());
      assertFalse(provider.isTerminal());
      assertNoInheritedSocketRetry(provider);

      int before = provider.sent.size();
      second = exchange.connect(boundaryProducts()).test();
      assertSame(provider, exchange.provider);
      newBooks = books.test(); // The same deferred public Observable must bind fresh subjects.
      newTrades = trades.test();
      newBooks.assertNoErrors().assertNotComplete();
      newTrades.assertNoErrors().assertNotComplete();
      negotiateSocketAttempt(exchange, before);
      finishSocketAttempt(exchange, second, before);
      newBooks.assertValueCount(1).assertNoErrors();
      newTrades.assertNoValues().assertNoErrors(); // Replayed predecessor trade stays suppressed.
      provider.frame(SyntheticWire.diff(2, 102, SyntheticWire.order(1, true, "BTC", "99", "3")));
      SyntheticWire.Encoded side = SyntheticWire.tradeSide(new byte[20], BigInteger.ONE, null, null, "0");
      provider.frame(SyntheticWire.trades(BigInteger.valueOf(2), BigInteger.ZERO, BigInteger.ZERO,
          BigInteger.valueOf(102), BigInteger.ZERO,
          SyntheticWire.trade(BigInteger.valueOf(123457789), false, null, "ETH", "10.75", "3.5",
              "Close", side, side)).bytes);
      newBooks.awaitCount(2).assertValueCount(2).assertNoErrors();
      newTrades.awaitCount(1).assertValueCount(1).assertNoErrors();
      assertEquals(new BigDecimal("3"), newBooks.values().get(1).getBids().get(0).getOriginalAmount());
      assertEquals(new BigDecimal("10.75"), newTrades.values().get(0).getPrice());
      assertEquals("p2p:trades:102:1:0", newTrades.values().get(0).getId());
      assertEquals(1, exchange.serviceCreations.get());
      assertEquals(2, provider.socketAttempts.get());
      oldBooks.assertValueCount(1).assertError(error -> error == original);
      oldTrades.assertValueCount(1).assertError(error -> error == original);
    } finally {
      oldBooks.dispose(); oldTrades.dispose(); first.dispose();
      if (newBooks != null) newBooks.dispose();
      if (newTrades != null) newTrades.dispose();
      if (second != null) second.dispose();
      if (exchange.provider != null) exchange.provider.stop();
    }
  }

  @Test(timeout = 10000) public void hostManagedConcurrentPendingConnectsJoinAndShareExactTransientFailure() throws Exception {
    SocketFailureExchange exchange = new SocketFailureExchange(null);
    TestObserver<Void> first = exchange.connect(boundaryProducts()).test();
    TestObserver<Void> joined = null;
    TestObserver<Void> retry = null;
    IOException original = new IOException("shared pending socket attempt failed");
    try {
      negotiateSocketAttempt(exchange, 0);
      SocketFailureService provider = exchange.provider;
      long generation = exchange.getGeneration();
      // The first ordinary connect is still pending replay when the second caller joins it.
      joined = exchange.connect(boundaryProducts()).test();
      first.assertNotComplete().assertNoErrors();
      joined.assertNotComplete().assertNoErrors();
      assertSame(provider, exchange.provider);
      assertEquals(generation, exchange.getGeneration());
      assertEquals(1, exchange.serviceCreations.get());
      assertEquals(1, provider.socketAttempts.get());
      provider.channel.pipeline().fireExceptionCaught(original);
      provider.pump();
      first.awaitDone(1, TimeUnit.SECONDS).assertNotComplete().assertError(error -> error == original);
      joined.awaitDone(1, TimeUnit.SECONDS).assertNotComplete().assertError(error -> error == original);
      assertSame(original, exchange.getLastFailure());
      assertFalse("A retryable failure must not stop the cached provider", provider.isTerminal());
      assertNoInheritedSocketRetry(provider);

      int before = provider.sent.size();
      retry = exchange.connect(boundaryProducts()).test();
      assertSame(provider, exchange.provider);
      assertSocketMembership(provider);
      negotiateSocketAttempt(exchange, before);
      finishSocketAttempt(exchange, retry, before);
      assertEquals(1, exchange.serviceCreations.get());
      assertEquals(2, provider.socketAttempts.get());
      assertNoInheritedSocketRetry(provider);
      first.assertError(error -> error == original);
      joined.assertError(error -> error == original);
    } finally {
      first.dispose();
      if (joined != null) joined.dispose();
      if (retry != null) retry.dispose();
      if (exchange.provider != null) exchange.provider.stop();
    }
  }

  @Test(timeout = 10000) public void hostManagedPermanentConnectFailurePreservesOriginalCauseBeforeDeadlineWithoutRetry() throws Exception {
    SocketFailureExchange exchange = new SocketFailureExchange(null);
    TestScheduler deadline = new TestScheduler();
    AtomicReference<Throwable> failureAtNotification = new AtomicReference<>();
    TestObserver<Void> connection = exchange.connect(boundaryProducts())
        .doOnError(error -> failureAtNotification.set(exchange.getLastFailure()))
        .timeout(1, TimeUnit.SECONDS, deadline).test();
    try {
      negotiateSocketAttempt(exchange, 0);
      SocketFailureService provider = exchange.provider;
      provider.text("{\"channel\":\"errors\",\"code\":\"version_mismatch\","
          + "\"message\":\"requested binary version is unsupported\"}");
      P2pStreamingServiceTest.waitFor(() -> {
        provider.pump();
        return exchange.getLastFailure() != null;
      });
      Throwable original = exchange.getLastFailure();
      assertTrue(original instanceof IOException);
      assertTrue(original.getMessage().contains("version_mismatch"));
      connection.awaitDone(1, TimeUnit.SECONDS).assertNotComplete().assertError(error -> error == original);
      assertSame("Commit the original cause before notifying connect subscribers", original,
          failureAtNotification.get());
      assertTrue(provider.isTerminal());
      assertFalse(provider.channel.isOpen());
      assertNoInheritedSocketRetry(provider);
      deadline.advanceTimeBy(1, TimeUnit.SECONDS);
      connection.assertError(error -> error == original);
      // Permanent failure is not a new transport attempt or a generic 'service stopped' error.
      exchange.connect(boundaryProducts()).test().awaitDone(1, TimeUnit.SECONDS)
          .assertNotComplete().assertError(error -> error == original);
      assertSame(provider, exchange.provider);
      assertEquals(1, exchange.serviceCreations.get());
      assertEquals(1, provider.socketAttempts.get());
      assertNoInheritedSocketRetry(provider);
    } finally {
      connection.dispose();
      if (exchange.provider != null) exchange.provider.stop();
    }
  }

  @Test(timeout = 10000) public void hostManagedPermanentLiveFailureErrorsBothStreamsWithOriginalCauseWithoutRetry() throws Exception {
    SocketFailureExchange exchange = new SocketFailureExchange(null);
    TestObserver<Void> connection = exchange.connect(boundaryProducts()).test();
    AtomicReference<Throwable> bookFailureAtNotification = new AtomicReference<>();
    AtomicReference<Throwable> tradeFailureAtNotification = new AtomicReference<>();
    Observable<OrderBook> books = exchange.getStreamingMarketDataService().getOrderBook(BTC);
    Observable<Trade> trades = exchange.getStreamingMarketDataService().getTrades(ETH);
    TestObserver<OrderBook> bookObserver = books
        .doOnError(error -> bookFailureAtNotification.set(exchange.getLastFailure())).test();
    TestObserver<Trade> tradeObserver = trades
        .doOnError(error -> tradeFailureAtNotification.set(exchange.getLastFailure())).test();
    try {
      negotiateSocketAttempt(exchange, 0);
      finishSocketAttempt(exchange, connection, 0);
      bookObserver.assertValueCount(1).assertNoErrors();
      tradeObserver.assertValueCount(1).assertNoErrors();
      SocketFailureService provider = exchange.provider;
      provider.text("{\"channel\":\"errors\",\"code\":\"version_mismatch\","
          + "\"message\":\"binary version revoked after live replay\"}");
      P2pStreamingServiceTest.waitFor(() -> {
        provider.pump();
        return exchange.getLastFailure() != null;
      });
      Throwable original = exchange.getLastFailure();
      assertTrue(original instanceof IOException);
      assertTrue(original.getMessage().contains("version_mismatch"));
      bookObserver.awaitDone(1, TimeUnit.SECONDS).assertValueCount(1).assertNotComplete()
          .assertError(error -> error == original);
      tradeObserver.awaitDone(1, TimeUnit.SECONDS).assertValueCount(1).assertNotComplete()
          .assertError(error -> error == original);
      assertSame(original, bookFailureAtNotification.get());
      assertSame(original, tradeFailureAtNotification.get());
      connection.assertComplete().assertNoErrors(); // The already-LIVE connect cannot fail again.
      assertTrue(provider.isTerminal());
      assertFalse(provider.channel.isOpen());
      assertNoInheritedSocketRetry(provider);
      books.test().awaitDone(1, TimeUnit.SECONDS).assertError(error -> error == original);
      trades.test().awaitDone(1, TimeUnit.SECONDS).assertError(error -> error == original);
      exchange.connect(boundaryProducts()).test().awaitDone(1, TimeUnit.SECONDS)
          .assertNotComplete().assertError(error -> error == original);
      assertSame(provider, exchange.provider);
      assertEquals(1, exchange.serviceCreations.get());
      assertEquals(1, provider.socketAttempts.get());
      assertNoInheritedSocketRetry(provider);
    } finally {
      bookObserver.dispose(); tradeObserver.dispose(); connection.dispose();
      if (exchange.provider != null) exchange.provider.stop();
    }
  }

  @Test(timeout = 10000) public void hostManagedStaleHandlerErrorsCannotKillFreshSuccessorPublishers() throws Exception {
    SocketFailureExchange exchange = new SocketFailureExchange(null);
    TestObserver<Void> first = exchange.connect(boundaryProducts()).test();
    Observable<OrderBook> books = exchange.getStreamingMarketDataService().getOrderBook(BTC);
    Observable<Trade> trades = exchange.getStreamingMarketDataService().getTrades(ETH);
    TestObserver<OrderBook> oldBooks = books.test();
    TestObserver<Trade> oldTrades = trades.test();
    TestObserver<OrderBook> newBooks = null;
    TestObserver<Trade> newTrades = null;
    TestObserver<Void> second = null;
    IOException original = new IOException("predecessor failed before replay became live");
    try {
      negotiateSocketAttempt(exchange, 0);
      SocketFailureService provider = exchange.provider;
      EmbeddedChannel oldChannel = provider.channel;
      WebSocketClientHandler oldHandler = oldChannel.pipeline().get(WebSocketClientHandler.class);
      ChannelHandlerContext oldContext = oldChannel.pipeline().context(oldHandler);
      assertNotNull(oldHandler);
      assertNotNull(oldContext);
      oldChannel.pipeline().fireExceptionCaught(original);
      provider.pump();
      first.awaitDone(1, TimeUnit.SECONDS).assertError(error -> error == original);
      oldBooks.awaitDone(1, TimeUnit.SECONDS).assertNoValues().assertError(error -> error == original);
      oldTrades.awaitDone(1, TimeUnit.SECONDS).assertNoValues().assertError(error -> error == original);
      assertFalse(provider.isTerminal());
      assertNoInheritedSocketRetry(provider);

      int before = provider.sent.size();
      second = exchange.connect(boundaryProducts()).test();
      assertSame(provider, exchange.provider);
      newBooks = books.test();
      newTrades = trades.test();
      negotiateSocketAttempt(exchange, before);
      finishSocketAttempt(exchange, second, before);
      newBooks.assertValueCount(1).assertNotComplete().assertNoErrors();
      newTrades.assertValueCount(1).assertNotComplete().assertNoErrors();
      EmbeddedChannel successor = provider.channel;
      assertNotSame(oldChannel, successor);
      long generation = exchange.getGeneration();
      Throwable priorFailure = exchange.getLastFailure();
      // Invoke the retained native handler directly: firing into the removed old pipeline could
      // silently bypass the stale callback and give a false-positive isolation assertion.
      oldHandler.channelRead(oldContext, new TextWebSocketFrame(
          "{\"channel\":\"errors\",\"code\":\"version_mismatch\",\"message\":\"late predecessor error\"}"));
      oldHandler.exceptionCaught(oldContext, new IOException("late predecessor transport error"));
      provider.pump();
      assertEquals(generation, exchange.getGeneration());
      assertSame(priorFailure, exchange.getLastFailure());
      assertSame(successor, provider.channel);
      assertTrue(successor.isOpen());
      assertEquals(P2pStreamingService.Readiness.LIVE, exchange.getReadiness());
      newBooks.assertNotComplete().assertNoErrors();
      newTrades.assertNotComplete().assertNoErrors();

      provider.frame(SyntheticWire.diff(2, 102, SyntheticWire.order(1, true, "BTC", "99", "3")));
      SyntheticWire.Encoded side = SyntheticWire.tradeSide(new byte[20], BigInteger.ONE, null, null, "0");
      provider.frame(SyntheticWire.trades(BigInteger.valueOf(2), BigInteger.ZERO, BigInteger.ZERO,
          BigInteger.valueOf(102), BigInteger.ZERO,
          SyntheticWire.trade(BigInteger.valueOf(123457789), false, null, "ETH", "10.75", "3.5",
              "Close", side, side)).bytes);
      newBooks.awaitCount(2).assertValueCount(2).assertNotComplete().assertNoErrors();
      newTrades.awaitCount(2).assertValueCount(2).assertNotComplete().assertNoErrors();
      assertEquals(new BigDecimal("3"), newBooks.values().get(1).getBids().get(0).getOriginalAmount());
      assertEquals(new BigDecimal("10.75"), newTrades.values().get(1).getPrice());
      assertEquals("p2p:trades:102:1:0", newTrades.values().get(1).getId());
      assertEquals(1, exchange.serviceCreations.get());
      assertEquals(2, provider.socketAttempts.get());
      assertNoInheritedSocketRetry(provider);
      oldBooks.assertNoValues().assertError(error -> error == original);
      oldTrades.assertNoValues().assertError(error -> error == original);
    } finally {
      oldBooks.dispose(); oldTrades.dispose(); first.dispose();
      if (newBooks != null) newBooks.dispose();
      if (newTrades != null) newTrades.dispose();
      if (second != null) second.dispose();
      if (exchange.provider != null) exchange.provider.stop();
    }
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

  private static void assertNoInheritedSocketRetry(SocketFailureService provider) throws Exception {
    synchronized (inheritedSocketField("reconnectLock").get(provider)) {
      assertEquals("No inherited timer may be scheduled, even if later cancelled", 0,
          provider.scheduledReconnects.get());
      assertNull(inheritedSocketField("reconnectFuture").get(provider));
      assertNull(inheritedSocketField("reconnectTask").get(provider));
      assertNull(inheritedSocketField("reconnectSubscription").get(provider));
    }
  }

  private static void assertSocketMembership(SocketFailureService provider) {
    assertEquals(Collections.singleton("BTC"), provider.getBookCoins());
    assertEquals(Collections.singleton("ETH"), provider.getTradeCoins());
    Set<String> union = new HashSet<>(Collections.singleton("BTC"));
    union.add("ETH");
    assertEquals(union, provider.getSubscribedCoins());
  }

  private static void negotiateSocketAttempt(SocketFailureExchange exchange, int before) throws Exception {
    SocketFailureService provider = exchange.provider;
    provider.awaitMessages(before + 1);
    assertEquals("{\"method\":\"info\"}", provider.sent.get(before));
    provider.text(boundaryInfo());
    provider.awaitMessages(before + 3);
    assertEquals("{\"method\":\"esp\",\"version\":3}", provider.sent.get(before + 1));
    assertReplaySubscription(provider.sent.get(before + 2), null, "BTC");
    assertEquals(P2pStreamingService.Readiness.CATCHING_UP, exchange.getReadiness());
  }

  private static void finishSocketAttempt(SocketFailureExchange exchange, TestObserver<Void> connection,
      int before) throws Exception {
    SocketFailureService provider = exchange.provider;
    provider.frame(SyntheticWire.diff(1, 101, SyntheticWire.order(1, true, "BTC", "99", "2")));
    provider.frame(SyntheticWire.status(0, 1, 101, 0));
    provider.awaitMessages(before + 4);
    assertReplaySubscription(provider.sent.get(before + 3), "trades", "ETH");
    connection.assertNotComplete().assertNoErrors(); // Book replay alone must not complete connect.
    SyntheticWire.Encoded side = SyntheticWire.tradeSide(new byte[20], BigInteger.ONE, null, null, "0");
    provider.frame(SyntheticWire.trades(BigInteger.ONE, BigInteger.ZERO, BigInteger.ZERO,
        BigInteger.valueOf(101), BigInteger.ZERO,
        SyntheticWire.trade(BigInteger.valueOf(123456789), true, null, "ETH", "10.25", "2.5",
            "Open", side, side)).bytes);
    provider.frame(SyntheticWire.status(3, 1, 101, 0));
    P2pStreamingServiceTest.waitFor(() -> {
      provider.pump();
      return exchange.getReadiness() == P2pStreamingService.Readiness.LIVE;
    });
    connection.awaitDone(1, TimeUnit.SECONDS).assertComplete().assertNoErrors();
    assertTrue(provider.channel.isOpen());
  }

  private static Field inheritedSocketField(String name) throws Exception {
    Field field = NettyStreamingService.class.getDeclaredField(name);
    field.setAccessible(true);
    return field;
  }

  private static void assertAutoReconnectBeforeFirstSocketAttempt(Boolean configured, boolean expected)
      throws Exception {
    BoundaryExchange exchange = new BoundaryExchange(configured);
    TestObserver<Void> connection = exchange.connect(boundaryProducts()).test();
    try {
      negotiateBookReplay(exchange);
      assertEquals("AUTO_RECONNECT must be applied before opening the first socket",
          Boolean.valueOf(expected), exchange.autoReconnectAtFirstSocketAttempt);
      assertEquals(expected, inheritedAutoReconnect(exchange.provider));
      connection.assertNotComplete().assertNoErrors();
      assertEquals(0, exchange.disconnectCalls.get());
      assertTrue(exchange.provider.channel.isOpen());
    } finally {
      connection.dispose();
      if (exchange.provider != null) exchange.provider.stop();
    }
  }

  private static boolean inheritedAutoReconnect(P2pStreamingService provider) throws Exception {
    Field field = NettyStreamingService.class.getDeclaredField("autoReconnect");
    field.setAccessible(true);
    return field.getBoolean(provider);
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
    volatile Boolean autoReconnectAtFirstSocketAttempt;

    BoundaryExchange() {
      this(null);
    }

    BoundaryExchange(Boolean autoReconnect) {
      ExchangeSpecification spec = getDefaultExchangeSpecification();
      spec.setShouldLoadRemoteMetaData(false);
      spec.setOverrideWebsocketApiUri("wss://native-public.invalid/ws");
      spec.setExchangeSpecificParametersItem(P2P_WS_URI, "wss://offline.invalid/ws");
      spec.setExchangeSpecificParametersItem(P2P_SNAPSHOT_URI, "https://snapshot.invalid/book");
      if (autoReconnect != null) {
        spec.setExchangeSpecificParametersItem(StreamingExchange.AUTO_RECONNECT, autoReconnect);
      }
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
      }, 32) {
        @Override protected Completable openConnection() {
          return Completable.defer(() -> {
            if (autoReconnectAtFirstSocketAttempt == null) {
              autoReconnectAtFirstSocketAttempt = inheritedAutoReconnect(this);
            }
            return super.openConnection();
          });
        }
      };
      return provider;
    }

    @Override public Completable disconnect() {
      disconnectCalls.incrementAndGet();
      return super.disconnect();
    }
  }

  private static final class SocketFailureExchange extends P2pHyperliquidStreamingExchange {
    final AtomicInteger serviceCreations = new AtomicInteger();
    final IOException initialSocketFailure;
    final boolean pendingHandshake;
    SocketFailureService provider;

    SocketFailureExchange(IOException initialSocketFailure) {
      this(initialSocketFailure, false);
    }

    SocketFailureExchange(IOException initialSocketFailure, boolean pendingHandshake) {
      this.initialSocketFailure = initialSocketFailure;
      this.pendingHandshake = pendingHandshake;
      ExchangeSpecification spec = getDefaultExchangeSpecification();
      spec.setShouldLoadRemoteMetaData(false);
      // Numeric host avoids DNS; the registration seam supplies an entirely embedded transport.
      spec.setExchangeSpecificParametersItem(P2P_WS_URI,
          pendingHandshake ? "ws://127.0.0.1/ws" : "ws://offline.invalid/ws");
      spec.setExchangeSpecificParametersItem(P2P_SNAPSHOT_URI, "https://snapshot.invalid/book");
      spec.setExchangeSpecificParametersItem(StreamingExchange.AUTO_RECONNECT, false);
      applySpecification(spec);
    }

    @Override protected P2pStreamingService createP2pService(URI websocket, Set<String> books,
        Set<String> trades, P2pSnapshotClient snapshots) {
      serviceCreations.incrementAndGet();
      provider = new SocketFailureService(websocket, books, trades, initialSocketFailure, pendingHandshake);
      return provider;
    }
  }

  /** Real inherited initial-error/close callbacks, with registration and later sockets kept offline. */
  private static final class ResolverCompatibleEmbeddedChannel extends EmbeddedChannel {
    private EventLoop resolverLoop;

    ResolverCompatibleEmbeddedChannel(ChannelHandler... handlers) {
      super(handlers);
      EventLoop embeddedLoop = super.eventLoop();
      EventLoop loop = mock(EventLoop.class, delegatesTo(embeddedLoop));
      // Bootstrap's default resolver installs a termination listener even for a numeric address.
      // EmbeddedEventLoop does not implement this hook; keep all execution on its real task queue.
      Promise<Void> termination = new DefaultPromise<>(ImmediateEventExecutor.INSTANCE);
      doReturn(termination).when(loop).terminationFuture();
      resolverLoop = loop;
      closeFuture().addListener(ignored -> termination.trySuccess(null));
    }

    @Override public EventLoop eventLoop() {
      // EmbeddedChannel registers itself during its super-constructor, before the wrapper exists.
      EventLoop loop = resolverLoop;
      return loop == null ? super.eventLoop() : loop;
    }
  }

  private static final class SocketFailureService extends P2pStreamingService {
    final AtomicInteger socketAttempts = new AtomicInteger();
    final AtomicInteger registrationAttempts = new AtomicInteger();
    final AtomicInteger scheduledReconnects = new AtomicInteger();
    final List<String> sent = new CopyOnWriteArrayList<>();
    final List<EmbeddedChannel> channelsCreated = new CopyOnWriteArrayList<>();
    final IOException initialSocketFailure;
    final boolean pendingHandshake;
    volatile EmbeddedChannel channel;
    volatile WebSocketClientHandler handshakeHandler;
    volatile ChannelFuture handshakeTransport;

    SocketFailureService(URI websocket, Set<String> books, Set<String> trades, IOException initialSocketFailure) {
      this(websocket, books, trades, initialSocketFailure, false);
    }

    SocketFailureService(URI websocket, Set<String> books, Set<String> trades,
        IOException initialSocketFailure, boolean pendingHandshake) {
      super(websocket, books, trades, new P2pWireCodec(),
          (coins, minimum) -> SyntheticWire.seed(100,
              SyntheticWire.order(1, true, "BTC", "99", "1"),
              SyntheticWire.order(2, false, "BTC", "101", "1")), 32);
      this.initialSocketFailure = initialSocketFailure;
      this.pendingHandshake = pendingHandshake;
      NioEventLoopGroup group = mock(NioEventLoopGroup.class);
      EventLoop loop = mock(EventLoop.class);
      when(group.next()).thenReturn(loop);
      when(group.register(any(Channel.class))).thenAnswer(invocation -> {
        registrationAttempts.incrementAndGet();
        if (pendingHandshake) {
          // Bootstrap continues with the registration future's embedded channel, never the unused
          // Nio channel. Preserve its real connect/close listeners and real pending handshake future.
          Channel unused = invocation.getArgument(0);
          unused.unsafe().closeForcibly();
          assertNotNull(handshakeHandler);
          channel = new ResolverCompatibleEmbeddedChannel(new ChannelOutboundHandlerAdapter() {
            @Override public void connect(ChannelHandlerContext ctx, SocketAddress remote,
                SocketAddress local, ChannelPromise promise) {
              handshakeTransport = promise;
              ctx.connect(remote, local, promise);
            }
          }, new HttpClientCodec(), handshakeHandler);
          channelsCreated.add(channel);
          return new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE).setSuccess();
        }
        // Failed registration stops Bootstrap before DNS/connect and makes it forcibly close
        // its unused Nio channel. Return an offline transport with a bound event loop so the
        // inherited failure/close path can run without an unrelated "channel not registered" error.
        EmbeddedChannel failedTransport = new EmbeddedChannel();
        // Keep closeFuture pending until Netty observes the original failed transport future.
        return new DefaultChannelPromise(failedTransport, ImmediateEventExecutor.INSTANCE)
            .setFailure(initialSocketFailure);
      });
      doAnswer(invocation -> {
        scheduledReconnects.incrementAndGet();
        return mock(io.netty.util.concurrent.ScheduledFuture.class);
      }).when(loop).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
      doReturn(ImmediateEventExecutor.INSTANCE.newSucceededFuture(null)).when(group)
          .shutdownGracefully(anyLong(), anyLong(), any(TimeUnit.class));
      try {
        inheritedSocketField("eventLoopGroup").set(this, group);
      } catch (Exception error) {
        throw new AssertionError(error);
      }
    }

    @Override protected WebSocketClientHandler getWebSocketClientHandler(
        WebSocketClientHandshaker handshaker, WebSocketClientHandler.WebSocketMessageHandler handler) {
      WebSocketClientHandler created = super.getWebSocketClientHandler(handshaker, handler);
      if (pendingHandshake) handshakeHandler = created;
      return created;
    }

    @Override protected Completable openConnection() {
      return Completable.defer(() -> {
        int attempt = socketAttempts.incrementAndGet();
        if (pendingHandshake || (attempt == 1 && initialSocketFailure != null)) {
          return super.openConnection();
        }
        return Completable.fromAction(() -> {
          WebSocketClientHandshaker handshaker = mock(WebSocketClientHandshaker.class);
          when(handshaker.isHandshakeComplete()).thenReturn(true);
          WebSocketClientHandler handler = getWebSocketClientHandler(handshaker, this::messageHandler);
          channel = new EmbeddedChannel(handler);
          channelsCreated.add(channel);
          // Bind the inherited channel fence so channelInactive exercises native retry ownership,
          // rather than silently returning as it does for an unbound embedded channel.
          inheritedSocketField("webSocketChannel").set(this, channel);
        });
      });
    }

    @Override protected void sendControl(String message, long token) {
      if (token == getGeneration()) sent.add(message);
    }

    void awaitMessages(int count) throws Exception {
      P2pStreamingServiceTest.waitFor(() -> {
        pump();
        return sent.size() == count;
      });
    }

    void text(String text) {
      pump();
      channel.writeInbound(new TextWebSocketFrame(text));
      pump();
    }

    void frame(byte[] bytes) throws Exception {
      Field queued = P2pStreamingService.class.getDeclaredField("queuedFrames");
      queued.setAccessible(true);
      java.util.concurrent.atomic.AtomicLong outstanding =
          (java.util.concurrent.atomic.AtomicLong) queued.get(this);
      pump();
      channel.writeInbound(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(bytes)));
      // Worker completion schedules the next manual read; embedded event loops need pumping
      // until flow-queued frames have also reached the worker, not merely until writeInbound returns.
      P2pStreamingServiceTest.waitFor(() -> {
        pump();
        return outstanding.get() == 0;
      });
      pump();
    }

    void pump() {
      for (EmbeddedChannel created : channelsCreated) {
        created.runPendingTasks();
        created.runScheduledPendingTasks();
      }
    }

    void stop() {
      try {
        TestObserver<Void> stopped = disconnect().test();
        pump();
        stopped.awaitDone(3, TimeUnit.SECONDS).assertComplete().assertNoErrors();
      } finally {
        for (EmbeddedChannel created : channelsCreated) created.finishAndReleaseAll();
      }
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
