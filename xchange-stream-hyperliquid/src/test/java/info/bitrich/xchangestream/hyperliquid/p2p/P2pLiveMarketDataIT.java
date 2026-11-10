package info.bitrich.xchangestream.hyperliquid.p2p;

import static org.junit.Assume.assumeTrue;

import info.bitrich.xchangestream.core.ProductSubscription;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.io.InputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.derivative.FuturesContract;
import org.knowm.xchange.dto.Order.OrderType;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Trade;
import org.knowm.xchange.dto.trade.LimitOrder;

/**
 * Explicit public-only acceptance, never an ordinary CI dependency.
 *
 * <p>Select this test explicitly with p2p.live=true, p2p.live.wsUri and
 * p2p.live.snapshotUri. The operator must serialize all live entrants and account for the shared
 * snapshot quota (4/hour, 15/day, including redirects/retries). No endpoints or credentials default.
 */
public class P2pLiveMarketDataIT {
  private static final long STARTUP_SECONDS = 30;
  private static final long RUNTIME_SECONDS = 65;
  private static final FuturesContract INSTRUMENT = new FuturesContract("HYPE/USD/PERP");

  @Test(timeout = 80000)
  public void publicBooksAndTradesReachLive() throws Throwable {
    assumeTrue("Requires p2p.live=true", "true".equals(System.getProperty("p2p.live")));
    BudgetExchange exchange = new BudgetExchange();
    CompositeDisposable subscriptions = new CompositeDisposable();
    Capture capture = new Capture();
    AtomicBoolean connected = new AtomicBoolean();
    long started = System.nanoTime();
    Throwable failure = null;
    try {
      URI websocket = endpoint(System.getProperty("p2p.live.wsUri"), true);
      URI snapshot = endpoint(System.getProperty("p2p.live.snapshotUri"), false);
      provenance(P2pHyperliquidStreamingExchange.class);
      provenance(P2pStreamingService.class);
      ExchangeSpecification specification = new ExchangeSpecification(P2pHyperliquidStreamingExchange.class);
      specification.setShouldLoadRemoteMetaData(false);
      specification.setExchangeSpecificParametersItem(P2pHyperliquidStreamingExchange.P2P_WS_URI, websocket.toString());
      specification.setExchangeSpecificParametersItem(P2pHyperliquidStreamingExchange.P2P_SNAPSHOT_URI, snapshot.toString());
      exchange.applySpecification(specification);
      subscriptions.add(exchange.connect(ProductSubscription.create()
              .addOrderbook(INSTRUMENT).addTrades(INSTRUMENT).build())
          .subscribeOn(Schedulers.io())
          .timeout(STARTUP_SECONDS, TimeUnit.SECONDS)
          .subscribe(() -> connected.set(true), capture::fail));

      boolean observing = false;
      while (System.nanoTime() - started < TimeUnit.SECONDS.toNanos(RUNTIME_SECONDS)) {
        capture.checkFailure();
        require(exchange.getLastFailure() == null, "Provider failure: " + exchange.getLastFailure());
        if (!connected.get()) {
          require(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(STARTUP_SECONDS),
              "Public provider startup deadline expired");
        } else if (!observing) {
          subscriptions.add(exchange.getStreamingMarketDataService().getOrderBook(INSTRUMENT)
              .subscribe(capture::book, capture::fail,
                  () -> capture.fail(new AssertionError("Book stream completed unexpectedly"))));
          subscriptions.add(exchange.getStreamingMarketDataService().getTrades(INSTRUMENT)
              .subscribe(capture::trade, capture::fail,
                  () -> capture.fail(new AssertionError("Trade stream completed unexpectedly"))));
          observing = true;
        }
        if (capture.accepted() && exchange.getReadiness() == P2pStreamingService.Readiness.LIVE) break;
        TimeUnit.MILLISECONDS.sleep(10);
      }
      capture.checkFailure();
      require(connected.get() && capture.accepted(), "Insufficient valid public events: " + capture.summary());
      require(exchange.getReadiness() == P2pStreamingService.Readiness.LIVE, "Provider is not LIVE");
      require(exchange.getLastFailure() == null, "Provider failure: " + exchange.getLastFailure());
      require(exchange.snapshotAttempts.get() == 1, "Expected exactly one real snapshot HTTP attempt");
    } catch (Throwable error) {
      failure = error;
    } finally {
      long cleanupStarted = System.nanoTime();
      System.out.println("P2P_LIVE_PROVIDER " + capture.summary() + " readiness=" + exchange.getReadiness()
          + " generation=" + exchange.getGeneration() + " providerFailure=" + exchange.getLastFailure()
          + " acceptanceFailure=" + (failure == null ? capture.failure.get() : failure)
          + " elapsedMs=" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
      try {
        subscriptions.dispose();
      } catch (Throwable error) {
        failure = append(failure, error);
      }
      try {
        exchange.disconnect().subscribeOn(Schedulers.io()).timeout(5, TimeUnit.SECONDS).blockingAwait();
        require(exchange.getReadiness() == P2pStreamingService.Readiness.DISCONNECTED && !exchange.isAlive(),
            "Provider did not stop and close its socket");
        System.out.println("P2P_LIVE_PROVIDER shutdown=DISCONNECTED");
      } catch (Throwable error) {
        failure = append(failure, error);
      }
      long cleanupElapsed = System.nanoTime() - cleanupStarted;
      if (cleanupElapsed > TimeUnit.SECONDS.toNanos(10)) {
        failure = append(failure, new AssertionError("Cleanup deadline exceeded"));
      }
      System.out.println("P2P_LIVE_PROVIDER cleanupMs=" + TimeUnit.NANOSECONDS.toMillis(cleanupElapsed));
      System.out.println("P2P_LIVE_PROVIDER snapshotHttpAttempts=" + exchange.snapshotAttempts.get()
          + " rejectedAttempts=" + exchange.snapshotRejected.get() + " budget=1");
      if (exchange.snapshotRejected.get() != 0) {
        failure = append(failure, new AssertionError("Snapshot HTTP attempt budget exhausted"));
      }
    }
    // An asynchronous callback racing final acceptance/cleanup must not be silently swallowed.
    failure = append(failure, capture.failure.get());
    if (failure != null) throw failure;
  }

  /** Only the HTTP resource budget is wrapped; all bytes and production transport behavior are real. */
  private static final class BudgetExchange extends P2pHyperliquidStreamingExchange {
    final AtomicInteger snapshotAttempts = new AtomicInteger();
    final AtomicInteger snapshotRejected = new AtomicInteger();

    @Override protected P2pStreamingService createP2pService(URI websocket, Set<String> books,
        Set<String> trades, P2pSnapshotClient snapshots) {
      require(snapshots != null, "Book acceptance requires the real snapshot client");
      try {
        Field field = P2pSnapshotClient.class.getDeclaredField("transport");
        field.setAccessible(true);
        P2pSnapshotClient.Transport real = (P2pSnapshotClient.Transport) field.get(snapshots);
        field.set(snapshots, (P2pSnapshotClient.Transport) (uri, maxBytes) -> {
          if (!snapshotAttempts.compareAndSet(0, 1)) {
            snapshotRejected.incrementAndGet();
            throw new IOException("Live acceptance allows only one snapshot HTTP attempt");
          }
          return real.get(uri, maxBytes);
        });
      } catch (ReflectiveOperationException error) {
        throw new IllegalStateException("Cannot install snapshot HTTP budget before connect", error);
      }
      return super.createP2pService(websocket, books, trades, snapshots);
    }
  }

  private static final class Capture {
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final Set<String> tradeIds = new HashSet<>();
    private int books;

    void fail(Throwable error) { failure.compareAndSet(null, error); }

    synchronized void book(OrderBook book) {
      try {
        require(book != null && book.getTimeStamp() != null && book.getTimeStamp().getTime() > 0,
            "Missing book/timestamp");
        require(book.getBids() != null && !book.getBids().isEmpty()
            && book.getAsks() != null && !book.getAsks().isEmpty(), "Book must have both sides");
        for (LimitOrder order : book.getBids()) level(order, OrderType.BID);
        for (LimitOrder order : book.getAsks()) level(order, OrderType.ASK);
        books++;
      } catch (Throwable error) {
        fail(error);
      }
    }

    synchronized void trade(Trade trade) {
      try {
        require(trade != null && INSTRUMENT.equals(trade.getInstrument()), "Wrong trade instrument");
        require(trade.getTimestamp() != null && trade.getTimestamp().getTime() > 0, "Missing trade timestamp");
        require(trade.getPrice() != null && trade.getPrice().signum() > 0, "Invalid trade price");
        require(trade.getType() == OrderType.BID || trade.getType() == OrderType.ASK, "Invalid trade side");
        require(trade.getOriginalAmount() != null
            && trade.getOriginalAmount().signum() == (trade.getType() == OrderType.BID ? 1 : -1),
            "Invalid signed trade amount");
        require(trade.getId() != null && trade.getId().matches("p2p:trades:[0-9]+:[0-9]+:[0-9]+"),
            "Invalid P2P trade ID");
        require(tradeIds.add(trade.getId()), "Duplicate P2P trade ID");
      } catch (Throwable error) {
        fail(error);
      }
    }

    private void level(LimitOrder order, OrderType side) {
      require(order != null && INSTRUMENT.equals(order.getInstrument()) && order.getType() == side,
          "Wrong book instrument/side");
      require(order.getLimitPrice() != null && order.getLimitPrice().signum() > 0, "Invalid book price");
      require(order.getOriginalAmount() != null && order.getOriginalAmount().signum() > 0,
          "Invalid book amount");
    }

    synchronized boolean accepted() { return books >= 3 && tradeIds.size() >= 3; }
    synchronized String summary() { return "validBooks=" + books + " distinctTrades=" + tradeIds.size(); }
    void checkFailure() throws Throwable { if (failure.get() != null) throw failure.get(); }
  }

  private static URI endpoint(String text, boolean websocket) {
    require(text != null && !text.trim().isEmpty(), "Explicit public endpoint required");
    URI uri;
    try {
      uri = URI.create(text);
    } catch (IllegalArgumentException error) {
      throw new IllegalArgumentException("Malformed public endpoint");
    }
    String scheme = uri.getScheme();
    boolean permitted = websocket ? "ws".equalsIgnoreCase(scheme) || "wss".equalsIgnoreCase(scheme)
        : "http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme);
    require(permitted && uri.isAbsolute() && uri.getHost() != null && !uri.getHost().isEmpty()
        && uri.getRawUserInfo() == null && uri.getRawQuery() == null && uri.getRawFragment() == null
        && uri.getPort() != 0 && uri.getPort() >= -1 && uri.getPort() <= 65535,
        "Expected public endpoint without user-info, query or fragment");
    return uri;
  }

  private static void provenance(Class<?> type) throws Exception {
    require(type.getProtectionDomain().getCodeSource() != null, "Missing loaded code source");
    URL source = type.getProtectionDomain().getCodeSource().getLocation();
    require("file".equals(source.getProtocol()), "Expected local loaded code source");
    Path path = Paths.get(source.toURI());
    String classHash;
    try (InputStream input = type.getResourceAsStream("/" + type.getName().replace('.', '/') + ".class")) {
      classHash = sha256(input);
    }
    if (Files.isRegularFile(path)) {
      try (InputStream input = Files.newInputStream(path)) {
        System.out.println("PROVENANCE class=" + type.getName() + " source=" + source
            + " kind=JAR sha256=" + sha256(input) + " classSha256=" + classHash);
      }
    } else {
      require(Files.isDirectory(path), "Code source is neither a JAR nor a classes directory");
      System.out.println("PROVENANCE class=" + type.getName() + " source=" + source
          + " kind=CLASSES_DIRECTORY jarSha256=NOT_APPLICABLE classSha256=" + classHash);
    }
  }

  private static String sha256(InputStream input) throws Exception {
    require(input != null, "Missing loaded class bytes");
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    byte[] buffer = new byte[8192];
    int count;
    while ((count = input.read(buffer)) != -1) digest.update(buffer, 0, count);
    StringBuilder hex = new StringBuilder();
    for (byte value : digest.digest()) hex.append(String.format(Locale.ROOT, "%02x", value & 0xff));
    return hex.toString();
  }

  private static Throwable append(Throwable first, Throwable next) {
    if (next == null) return first;
    if (first == null) return next;
    if (first != next) first.addSuppressed(next);
    return first;
  }

  private static void require(boolean condition, String message) {
    if (!condition) throw new AssertionError(message);
  }
}
