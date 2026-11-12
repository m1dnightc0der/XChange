package info.bitrich.xchangestream.hyperliquid.p2p;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

/** Synthetic fixtures sent through the public client's real HTTP transport, never a mock transport. */
public class P2pSnapshotHttpTransportTest {
  private static final BigInteger MIN_HEIGHT = BigInteger.valueOf(42);
  private static final long SNAPSHOT_HEIGHT = 47;
  private static final Set<String> BTC = Collections.singleton("BTC");

  @Test(timeout = 10000)
  public void sendsExactEncodedGetAndAcceptHeadersAndDecodesSnapshot() throws Exception {
    Set<String> coins = new LinkedHashSet<>(Arrays.asList("BTC/US D+?&=\u00e9", "ETH%#"));
    byte[] body = snapshot(coins);
    try (LoopbackServer server = new LoopbackServer(exchange -> respond(exchange, "47", "identity", body, false))) {
      P2pWireCodec.Snapshot result = server.fetch(
          server.uri("/v3/snapshot?tenant=offline%2Btest"), Collections.emptySet(),
          new P2pWireCodec(), coins);

      assertEquals(BigInteger.valueOf(SNAPSHOT_HEIGHT), result.height);
      assertEquals(coins, result.books.keySet());
      assertTrue(result.books.values().stream().allMatch(List::isEmpty));
      assertRequest(server.takeRequest(),
          "/v3/snapshot?tenant=offline%2Btest&min_height=42"
              + "&coins=BTC%2FUS%20D%2B%3F%26%3D%C3%A9%2CETH%25%23");
      assertTrue(server.requests.isEmpty());
    }
  }

  @Test(timeout = 10000)
  public void followsRelativeSameOrigin307() throws Exception {
    byte[] body = snapshot(BTC);
    try (LoopbackServer server = new LoopbackServer(exchange -> {
      if ("/v3/snapshot".equals(exchange.getRequestURI().getPath())) {
        redirect(exchange, "next?min_height=42&coins=BTC");
      } else {
        respond(exchange, "47", null, body, false);
      }
    })) {
      P2pWireCodec.Snapshot result = server.fetch(server.uri("/v3/snapshot"),
          Collections.emptySet(), new P2pWireCodec(), BTC);

      assertEquals(BigInteger.valueOf(SNAPSHOT_HEIGHT), result.height);
      assertEquals(BTC, result.books.keySet());
      assertRequest(server.takeRequest(), "/v3/snapshot?min_height=42&coins=BTC");
      assertRequest(server.takeRequest(), "/v3/next?min_height=42&coins=BTC");
      assertTrue(server.requests.isEmpty());
    }
  }

  @Test(timeout = 10000)
  public void refusesUnapprovedCrossOriginWithoutRequestingTarget() throws Exception {
    byte[] body = snapshot(BTC);
    try (LoopbackServer target = new LoopbackServer(exchange -> respond(exchange, "47", null, body, false));
        LoopbackServer source = new LoopbackServer(exchange ->
            redirect(exchange, target.uri("/landing?min_height=42&coins=BTC").toString()))) {
      IOException failure = assertThrows(IOException.class, () -> source.fetch(
          source.uri("/snapshot"), Collections.emptySet(), new P2pWireCodec(), BTC));

      assertEquals("Unapproved snapshot redirect", failure.getMessage());
      assertRequest(source.takeRequest(), "/snapshot?min_height=42&coins=BTC");
      // fetch has completed synchronously: there is no outstanding redirect to race here.
      assertTrue("A rejected origin must receive no HTTP request", target.requests.isEmpty());
      assertTrue(source.requests.isEmpty());
    }
  }

  @Test(timeout = 10000)
  public void followsExplicitlyApprovedCrossOrigin307() throws Exception {
    byte[] body = snapshot(BTC);
    try (LoopbackServer target = new LoopbackServer(exchange -> respond(exchange, "47", null, body, false));
        LoopbackServer source = new LoopbackServer(exchange ->
            redirect(exchange, target.uri("/landing?min_height=42&coins=BTC").toString()))) {
      P2pWireCodec.Snapshot result = source.fetch(source.uri("/snapshot"),
          Collections.singleton(target.uri("/")), new P2pWireCodec(), BTC);

      assertEquals(BigInteger.valueOf(SNAPSHOT_HEIGHT), result.height);
      assertEquals(BTC, result.books.keySet());
      assertRequest(source.takeRequest(), "/snapshot?min_height=42&coins=BTC");
      assertRequest(target.takeRequest(), "/landing?min_height=42&coins=BTC");
      assertTrue(source.requests.isEmpty());
      assertTrue(target.requests.isEmpty());
    }
  }

  @Test(timeout = 10000)
  public void rejectsMissingSnapshotHeightHeader() throws Exception {
    byte[] body = snapshot(BTC);
    try (LoopbackServer server = new LoopbackServer(exchange -> respond(exchange, null, null, body, false))) {
      IOException failure = assertThrows(IOException.class, () -> server.fetch(
          server.uri("/snapshot"), Collections.emptySet(), new P2pWireCodec(), BTC));
      assertEquals("Missing snapshot height", failure.getMessage());
      assertRequest(server.takeRequest(), "/snapshot?min_height=42&coins=BTC");
    }
  }

  @Test(timeout = 10000)
  public void rejectsMalformedOutOfRangeAndInconsistentHeightHeaders() throws Exception {
    byte[] body = snapshot(BTC);
    // Negative, nonnumeric, uint64 overflow, below min_height, and body/header disagreement.
    for (String height : Arrays.asList("-1", "not-height", "18446744073709551616", "41", "48")) {
      try (LoopbackServer server = new LoopbackServer(exchange -> respond(exchange, height, null, body, false))) {
        IOException failure = assertThrows(height, IOException.class, () -> server.fetch(
            server.uri("/snapshot"), Collections.emptySet(), new P2pWireCodec(), BTC));
        assertEquals(height, height.equals("-1") || height.equals("not-height")
            ? "Missing snapshot height" : "Invalid snapshot", failure.getMessage());
        assertRequest(server.takeRequest(), "/snapshot?min_height=42&coins=BTC");
      }
    }
  }

  @Test(timeout = 10000)
  public void rejectsUnexpectedResponseCompression() throws Exception {
    byte[] body = snapshot(BTC);
    try (LoopbackServer server = new LoopbackServer(exchange ->
        rejectedResponse(exchange, "gzip", body, false))) {
      IOException failure = assertThrows(IOException.class, () -> server.fetch(
          server.uri("/snapshot"), Collections.emptySet(), new P2pWireCodec(), BTC));
      assertEquals("Unexpected snapshot compression", failure.getMessage());
      assertRequest(server.takeRequest(), "/snapshot?min_height=42&coins=BTC");
    }
  }

  @Test(timeout = 10000)
  public void acceptsSnapshotExactlyAtConfiguredBodyLimit() throws Exception {
    Set<String> coins = new LinkedHashSet<>(Arrays.asList("BTC", "ETH"));
    byte[] body = snapshot(coins);
    P2pWireCodec codec = new P2pWireCodec(body.length, body.length, 10);
    try (LoopbackServer server = new LoopbackServer(exchange -> respond(exchange, "47", null, body, false))) {
      P2pWireCodec.Snapshot result = server.fetch(server.uri("/snapshot"),
          Collections.emptySet(), codec, coins);
      assertEquals(BigInteger.valueOf(SNAPSHOT_HEIGHT), result.height);
      assertEquals(coins, result.books.keySet());
      assertRequest(server.takeRequest(), "/snapshot?min_height=42&coins=BTC%2CETH");
    }
  }

  @Test(timeout = 10000)
  public void rejectsContentLengthAboveConfiguredBodyLimit() throws Exception {
    assertOversizedBodyRejected(false);
  }

  @Test(timeout = 10000)
  public void rejectsChunkedBodyAboveConfiguredBodyLimit() throws Exception {
    assertOversizedBodyRejected(true);
  }

  @Test(timeout = 10000)
  public void fiftyCoinsUseOneRealGetWithCompleteDecodedMembership() throws Exception {
    Set<String> coins = new LinkedHashSet<>();
    for (int i = 0; i < 49; i++) coins.add("COIN" + i);
    coins.add("#102170");
    try (LoopbackServer server = new LoopbackServer(exchange ->
        respond(exchange, "47", null, snapshot(coins), false))) {
      P2pWireCodec.Snapshot result = server.fetch(server.uri("/v3/snapshot"),
          Collections.emptySet(), new P2pWireCodec(), coins, BigInteger.ZERO);
      assertEquals(coins, result.books.keySet());
      Request request = server.takeRequest();
      Map<String, String> query = new LinkedHashMap<>();
      for (String part : URI.create(request.target).getRawQuery().split("&")) {
        String[] pair = part.split("=", 2);
        assertNull(query.put(URLDecoder.decode(pair[0], "UTF-8"),
            URLDecoder.decode(pair[1], "UTF-8")));
      }
      assertEquals(2, query.size());
      assertEquals("0", query.get("min_height"));
      List<String> decodedCoins = Arrays.asList(query.get("coins").split(",", -1));
      assertEquals("No duplicate or extra query members", 50, decodedCoins.size());
      assertEquals(coins, new LinkedHashSet<>(decodedCoins));
      assertEquals("GET", request.method);
      assertTrue(server.requests.isEmpty());
    }
  }

  @Test(timeout = 10000)
  public void real400CodesSettleBothWindowsWithoutRetrying() throws Exception {
    AtomicReference<String> code = new AtomicReference<>("missing_min_height");
    AtomicLong clock = new AtomicLong();
    List<Long> waits = new ArrayList<>();
    try (LoopbackServer server = new LoopbackServer(exchange ->
        errorResponse(exchange, 400, null, "{\"code\":\"" + code.get() + "\"}", false))) {
      P2pSnapshotClient client = controlledClient(server, clock, waits);
      for (int i = 0; i < 20; i++) {
        code.set(i % 2 == 0 ? "missing_min_height" : "invalid_min_height");
        IOException error = assertThrows(IOException.class, () -> client.fetch(BTC, MIN_HEIGHT));
        assertEquals("Snapshot HTTP status 400", error.getMessage());
        server.takeRequest();
      }
      assertTrue("known-free 400s consume neither window", waits.isEmpty());
      for (int i = 0; i < 5; i++) {
        code.set(i % 2 == 0 ? "invalid_query" : "unknown_coin");
        assertThrows(IOException.class, () -> client.fetch(BTC, MIN_HEIGHT));
        server.takeRequest();
      }
      assertEquals(Collections.singletonList(3_600_000L), waits);
      assertTrue(server.requests.isEmpty());
    }
  }

  @Test(timeout = 10000)
  public void malformedAndOversizedOptionalBodiesPreserve429DeadlineAndDoNotRetryEarly()
      throws Exception {
    for (String body : Arrays.asList("{not-json", "{\"window\":\"day\"}",
        new String(new char[8192]).replace('\0', 'x'))) {
      AtomicInteger attempts = new AtomicInteger();
      AtomicLong clock = new AtomicLong();
      List<Long> waits = new ArrayList<>();
      try (LoopbackServer server = new LoopbackServer(exchange -> {
        if (attempts.incrementAndGet() == 1) errorResponse(exchange, 429, "8672", body, true);
        else respond(exchange, "47", null, snapshot(BTC), false);
      })) {
        P2pWireCodec.Snapshot result = controlledClient(server, clock, waits).fetch(BTC, MIN_HEIGHT);
        assertEquals(BigInteger.valueOf(47), result.height);
        assertEquals(Collections.singletonList(8_672_000L), waits);
        assertEquals(2, attempts.get());
        server.takeRequest();
        server.takeRequest();
        assertTrue(server.requests.isEmpty());
      }
    }
  }

  @Test(timeout = 10000)
  public void stalledOptionalBodyIsBoundedAndStillRetainsKnownStatusAndHeader() throws Exception {
    assertOptionalBodyBounded(false);
  }

  @Test(timeout = 10000)
  public void interruptedOptionalBodyStillRetainsKnownStatusAndHeader() throws Exception {
    assertOptionalBodyBounded(true);
  }

  @Test(timeout = 10000)
  public void chunkedStalledOptionalBodyDoesNotEnterAnUnboundedFramingRead() throws Exception {
    assertOptionalBodyBounded(false, true);
  }

  @Test(timeout = 10000)
  public void slowSuccessfulBodyStillUsesTheExistingReadTimeout() throws Exception {
    try (LoopbackServer server = new LoopbackServer(exchange -> {
      byte[] body = snapshot(BTC);
      exchange.getResponseHeaders().set("X-Snapshot-Height", "47");
      exchange.sendResponseHeaders(200, body.length);
      exchange.getResponseBody().write(body, 0, 1);
      exchange.getResponseBody().flush();
      try { Thread.sleep(700); }
      catch (InterruptedException stopped) { Thread.currentThread().interrupt(); }
      exchange.getResponseBody().write(body, 1, body.length - 1);
    })) {
      assertEquals(BTC, server.fetch(server.uri("/snapshot"), Collections.emptySet(),
          new P2pWireCodec(), BTC).books.keySet());
      server.takeRequest();
      assertTrue(server.requests.isEmpty());
    }
  }

  @Test(timeout = 10000)
  public void trickledOptionalBodyHasOneOverallBoundNotAResettingReadTimeout() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch sent = new CountDownLatch(1);
    try (LoopbackServer server = new LoopbackServer(exchange -> {
      exchange.getResponseHeaders().set("Retry-After", "8672");
      exchange.sendResponseHeaders(429, 100);
      try {
        exchange.getResponseBody().write('{');
        exchange.getResponseBody().flush();
        sent.countDown();
        for (int i = 0; i < 99 && !release.await(50, TimeUnit.MILLISECONDS); i++) {
          exchange.getResponseBody().write(' ');
          exchange.getResponseBody().flush();
        }
      } catch (IOException disconnected) {
        // The optional body is deliberately abandoned before this slow response completes.
      } catch (InterruptedException stopped) { Thread.currentThread().interrupt(); }
    })) {
      P2pSnapshotClient.Transport transport = realTransport(server);
      Future<P2pSnapshotClient.Response> result = server.clients.submit(() ->
          transport.get(server.uri("/snapshot"), 1024));
      try {
        assertTrue(sent.await(1, TimeUnit.SECONDS));
        P2pSnapshotClient.Response response = result.get(1500, TimeUnit.MILLISECONDS);
        assertEquals(429, response.status);
        assertEquals("8672", response.header("Retry-After"));
        assertEquals(0, response.body.length);
        server.takeRequest();
        assertTrue(server.requests.isEmpty());
      } finally {
        release.countDown();
        result.cancel(true);
      }
    }
  }

  @Test(timeout = 10000)
  public void known200BodyPolicyFailureIsLoggedAsCountedNotUnknown() throws Exception {
    Logger logger = (Logger) LoggerFactory.getLogger(P2pSnapshotClient.class);
    Level previous = logger.getLevel();
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.INFO);
    try (LoopbackServer server = new LoopbackServer(exchange ->
        rejectedResponse(exchange, "gzip", snapshot(BTC), false))) {
      IOException failure = assertThrows(IOException.class, () -> server.fetch(
          server.uri("/snapshot"), Collections.emptySet(), new P2pWireCodec(), BTC));
      assertEquals("Unexpected snapshot compression", failure.getMessage());
      List<ILoggingEvent> results = new ArrayList<>();
      for (ILoggingEvent event : appender.list) {
        if (event.getMessage().startsWith("P2P snapshot response attempt_id=")) results.add(event);
      }
      assertEquals(1, results.size());
      assertEquals("200", String.valueOf(results.get(0).getArgumentArray()[1]));
      assertEquals("counted", String.valueOf(results.get(0).getArgumentArray()[4]));
      server.takeRequest();
      assertTrue(server.requests.isEmpty());
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(previous);
      appender.stop();
    }
  }

  private static void assertOptionalBodyBounded(boolean interrupt) throws Exception {
    assertOptionalBodyBounded(interrupt, false);
  }

  private static void assertOptionalBodyBounded(boolean interrupt, boolean chunked) throws Exception {
    CountDownLatch sent = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicReference<Thread> reader = new AtomicReference<>();
    try (LoopbackServer server = new LoopbackServer(exchange -> {
      exchange.getResponseHeaders().set("Retry-After", "8672");
      exchange.sendResponseHeaders(429, chunked ? 0 : 100);
      exchange.getResponseBody().write('{');
      exchange.getResponseBody().flush();
      sent.countDown();
      try { release.await(3, TimeUnit.SECONDS); }
      catch (InterruptedException stopped) { Thread.currentThread().interrupt(); }
    })) {
      P2pSnapshotClient.Transport transport = realTransport(server);
      Future<P2pSnapshotClient.Response> result = server.clients.submit(() -> {
        reader.set(Thread.currentThread());
        return transport.get(server.uri("/snapshot"), 1024);
      });
      try {
        assertTrue(sent.await(1, TimeUnit.SECONDS));
        if (interrupt) reader.get().interrupt();
        P2pSnapshotClient.Response response = result.get(1500, TimeUnit.MILLISECONDS);
        assertEquals(429, response.status);
        assertEquals("8672", response.header("Retry-After"));
        assertEquals("incomplete optional JSON must not be consumed", 0, response.body.length);
        assertTrue(server.requests.size() == 1);
      } finally {
        release.countDown();
        result.cancel(true);
      }
    }
  }

  private static P2pSnapshotClient controlledClient(LoopbackServer server, AtomicLong clock,
      List<Long> waits) throws Exception {
    return new P2pSnapshotClient(server.uri("/snapshot"), Collections.emptySet(),
        new P2pWireCodec(), realTransport(server), clock::get, delay -> {
          waits.add(delay);
          clock.addAndGet(delay);
        });
  }

  private static P2pSnapshotClient.Transport realTransport(LoopbackServer server) throws Exception {
    P2pSnapshotClient client = new P2pSnapshotClient(server.uri("/snapshot"),
        Collections.emptySet(), new P2pWireCodec());
    java.lang.reflect.Field field = P2pSnapshotClient.class.getDeclaredField("transport");
    field.setAccessible(true);
    return (P2pSnapshotClient.Transport) field.get(client);
  }

  private static void errorResponse(HttpExchange exchange, int status, String retryAfter,
      String text, boolean chunked) throws IOException {
    byte[] body = text.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    if (retryAfter != null) exchange.getResponseHeaders().set("Retry-After", retryAfter);
    exchange.sendResponseHeaders(status, chunked ? 0 : body.length);
    try (OutputStream out = exchange.getResponseBody()) { out.write(body); }
    catch (IOException earlyDisconnect) { /* Optional diagnostics may be abandoned. */ }
  }

  private static void assertOversizedBodyRejected(boolean chunked) throws Exception {
    byte[] body = new byte[129];
    P2pWireCodec codec = new P2pWireCodec(128, 128, 10);
    try (LoopbackServer server = new LoopbackServer(exchange ->
        rejectedResponse(exchange, null, body, chunked))) {
      IOException failure = assertThrows(IOException.class, () -> server.fetch(
          server.uri("/snapshot"), Collections.emptySet(), codec, BTC));
      assertEquals("Snapshot body limit", failure.getMessage());
      assertRequest(server.takeRequest(), "/snapshot?min_height=42&coins=BTC");
    }
  }

  private static byte[] snapshot(Set<String> coins) {
    Map<String, List<P2pWireCodec.Change>> books = new LinkedHashMap<>();
    coins.forEach(coin -> books.put(coin, Collections.emptyList()));
    return SyntheticWire.snapshot(SNAPSHOT_HEIGHT, books);
  }

  private static void assertRequest(Request request, String target) {
    assertEquals("GET", request.method);
    assertEquals(target, request.target);
    assertEquals("application/octet-stream", request.accept);
    assertEquals("identity", request.acceptEncoding);
  }

  private static void redirect(HttpExchange exchange, String location) throws IOException {
    exchange.getResponseHeaders().set("Location", location);
    exchange.sendResponseHeaders(307, -1);
  }

  private static void respond(HttpExchange exchange, String height, String encoding,
      byte[] body, boolean chunked) throws IOException {
    exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
    // Deliberately mixed case: response header lookup must be case insensitive.
    if (height != null) exchange.getResponseHeaders().set("x-SnApShOt-HeIgHt", height);
    if (encoding != null) exchange.getResponseHeaders().set("Content-Encoding", encoding);
    exchange.sendResponseHeaders(200, chunked ? 0 : body.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(body);
    }
  }

  private static void rejectedResponse(HttpExchange exchange, String encoding,
      byte[] body, boolean chunked) throws IOException {
    try {
      respond(exchange, "47", encoding, body, chunked);
    } catch (IOException earlyDisconnect) {
      // Header/size rejection may close the socket before the server finishes writing.
      // Tests still require the client's exact rejection, not a generic connection failure.
    }
  }

  private static final class Request {
    final String method;
    final String target;
    final String accept;
    final String acceptEncoding;

    Request(HttpExchange exchange) {
      method = exchange.getRequestMethod();
      target = exchange.getRequestURI().toASCIIString();
      accept = exchange.getRequestHeaders().getFirst("Accept");
      acceptEncoding = exchange.getRequestHeaders().getFirst("Accept-Encoding");
    }
  }

  private static final class LoopbackServer implements AutoCloseable {
    final BlockingQueue<Request> requests = new LinkedBlockingQueue<>();
    final AtomicReference<Throwable> handlerFailure = new AtomicReference<>();
    final HttpServer server;
    final ExecutorService handlers = worker("snapshot-http-server");
    final ExecutorService clients = worker("snapshot-http-client");

    LoopbackServer(HttpHandler handler) throws IOException {
      // A literal IPv4 loopback address avoids DNS and public/wildcard listener bindings.
      server = HttpServer.create(new InetSocketAddress(
          InetAddress.getByAddress(new byte[] {127, 0, 0, 1}), 0), 0);
      server.setExecutor(handlers);
      server.createContext("/", exchange -> {
        try {
          requests.add(new Request(exchange));
          handler.handle(exchange);
        } catch (Throwable failure) {
          handlerFailure.compareAndSet(null, failure);
        } finally {
          exchange.close();
        }
      });
      server.start();
    }

    URI uri(String path) {
      return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + path);
    }

    P2pWireCodec.Snapshot fetch(URI endpoint, Set<URI> approvedOrigins,
        P2pWireCodec codec, Set<String> coins) throws Exception {
      return fetch(endpoint, approvedOrigins, codec, coins, MIN_HEIGHT);
    }

    P2pWireCodec.Snapshot fetch(URI endpoint, Set<URI> approvedOrigins,
        P2pWireCodec codec, Set<String> coins, BigInteger minHeight) throws Exception {
      // Only this public constructor: no injected Transport, clock, or sleeper.
      P2pSnapshotClient client = new P2pSnapshotClient(endpoint, approvedOrigins, codec);
      Future<P2pWireCodec.Snapshot> result = clients.submit(() -> client.fetch(coins, minHeight));
      try {
        return result.get(3, TimeUnit.SECONDS);
      } catch (ExecutionException failure) {
        Throwable cause = failure.getCause();
        if (cause instanceof Exception) throw (Exception) cause;
        throw new AssertionError("HTTP worker failed", cause);
      } finally {
        result.cancel(true);
      }
    }

    Request takeRequest() throws InterruptedException {
      Request request = requests.poll(1, TimeUnit.SECONDS);
      assertTrue("Expected a loopback HTTP request", request != null);
      return request;
    }

    @Override
    public void close() throws InterruptedException {
      // Close sockets first so a blocked HttpURLConnection is released before joining workers.
      server.stop(0);
      clients.shutdownNow();
      handlers.shutdownNow();
      boolean clientStopped = clients.awaitTermination(2, TimeUnit.SECONDS);
      boolean handlersStopped = handlers.awaitTermination(2, TimeUnit.SECONDS);
      assertTrue("HTTP client worker did not terminate", clientStopped);
      assertTrue("HTTP server worker did not terminate", handlersStopped);
      assertNull("Loopback handler failed", handlerFailure.get());
    }

    private static ExecutorService worker(String name) {
      return Executors.newSingleThreadExecutor(task -> {
        Thread thread = new Thread(task, name);
        thread.setDaemon(true);
        return thread;
      });
    }
  }
}
