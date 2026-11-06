package info.bitrich.xchangestream.hyperliquid.p2p;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import info.bitrich.xchangestream.hyperliquid.HyperliquidStreamingExchange;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Observable;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
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
        ProductSubscription.create().addTrades(BTC).build(),
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

  @Test public void independentNativePublicTradesKeepNativeUrisAndNeverReceiveBooksOrCredentials() {
    TestExchange exchange = new TestExchange();
    ExchangeSpecification spec = exchange.getDefaultExchangeSpecification();
    spec.setShouldLoadRemoteMetaData(false);
    spec.setOverrideWebsocketApiUri("wss://native-public.invalid/ws");
    spec.setExchangeSpecificParametersItem(P2pHyperliquidStreamingExchange.P2P_WS_URI, "wss://p2p.invalid/ws");
    spec.setExchangeSpecificParametersItem(P2pHyperliquidStreamingExchange.P2P_SNAPSHOT_URI, "https://snapshot.invalid/book");
    exchange.applySpecification(spec);
    ProductSubscription products = ProductSubscription.create().addOrderbook(BTC).addTicker(ETH).addTrades(BTC).build();
    exchange.connect(products).test().assertComplete();
    assertEquals(new HashSet<>(java.util.Arrays.asList("BTC", "ETH")), exchange.getSubscribedCoins());
    assertEquals("wss://p2p.invalid/ws", exchange.websocket.toString());
    assertEquals("wss://native-public.invalid/ws", exchange.nativeDelegate.spec.getOverrideWebsocketApiUri());
    assertFalse(exchange.getExchangeSpecification().getSslUri().contains("p2p"));
    assertFalse(exchange.nativeDelegate.spec.getSslUri().contains("p2p"));
    assertNull(exchange.nativeDelegate.spec.getApiKey());
    assertNull(exchange.nativeDelegate.spec.getSecretKey());
    assertNull(exchange.nativeDelegate.spec.getExchangeSpecificParametersItem(P2pHyperliquidStreamingExchange.P2P_WS_URI));
    assertEquals(Collections.singletonList(BTC), exchange.nativeDelegate.subscription.getTrades());
    assertTrue(exchange.nativeDelegate.subscription.getOrderBook().isEmpty());
    assertTrue(exchange.nativeDelegate.subscription.getTicker().isEmpty());
    when(exchange.nativeDelegate.market.getTrades(BTC)).thenReturn(Observable.<Trade>empty());
    exchange.getStreamingMarketDataService().getTrades(BTC).test().assertComplete();
    verify(exchange.nativeDelegate.market).getTrades(BTC);
    exchange.connect(ProductSubscription.create().addOrderbook(BTC).build()).test()
        .assertError(IllegalArgumentException.class);
    exchange.getStreamingMarketDataService().getOrderBook(new FuturesContract("SOL/USDC/PERP"))
        .test().assertError(IllegalArgumentException.class);
    exchange.disconnect().test().assertComplete();
    verify(exchange.stub).disconnect();
    assertTrue(exchange.nativeDelegate.disconnected);
  }

  private static final class TestExchange extends P2pHyperliquidStreamingExchange {
    final FakeNative nativeDelegate = new FakeNative();
    P2pStreamingService stub;
    URI websocket;
    @Override protected HyperliquidStreamingExchange createPublicDelegate() { return nativeDelegate; }
    @Override protected P2pStreamingService createP2pService(URI websocket, Set<String> coins, P2pSnapshotClient snapshots) {
      this.websocket = websocket;
      stub = mock(P2pStreamingService.class);
      when(stub.getSubscribedCoins()).thenReturn(coins);
      when(stub.getReadiness()).thenReturn(P2pStreamingService.Readiness.LIVE);
      when(stub.connect()).thenReturn(Completable.complete());
      when(stub.awaitLive()).thenReturn(Completable.complete());
      when(stub.disconnect()).thenReturn(Completable.complete());
      return stub;
    }
  }

  private static final class FakeNative extends HyperliquidStreamingExchange {
    ExchangeSpecification spec;
    ProductSubscription subscription;
    boolean disconnected;
    final StreamingMarketDataService market = mock(StreamingMarketDataService.class);
    @Override public void applySpecification(ExchangeSpecification spec) { this.spec = spec; }
    @Override public Completable connect(ProductSubscription... subscriptions) {
      subscription = subscriptions[0];
      return Completable.complete();
    }
    @Override public Completable disconnect() { disconnected = true; return Completable.complete(); }
    @Override public StreamingMarketDataService getStreamingMarketDataService() { return market; }
  }
}
