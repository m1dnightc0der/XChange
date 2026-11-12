package info.bitrich.xchangestream.hyperliquid.p2p;

import static org.junit.Assert.*;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class P2pSnapshotClientTest {
  private static final URI ENDPOINT = URI.create("https://snap.example/snapshot");
  private static final String REQUEST_LOG = "P2P snapshot request attempt_id={} reason={} coin_count={} min_height={} origin={} path={}";
  private static final String RESPONSE_LOG = "P2P snapshot response attempt_id={} status={} code={} window={} accounting={}";

  @Test public void hostedTransportFailureIsOriginalAndNeverRunsAnInternalRetry() throws Exception {
    AtomicLong clock = new AtomicLong(1_000_000L);
    AtomicInteger calls = new AtomicInteger();
    List<Long> sleeps = new ArrayList<>();
    SocketException original = new SocketException("controlled snapshot transport failure");
    P2pSnapshotClient client = client((uri, limit) -> {
      calls.incrementAndGet();
      throw original;
    }, clock, sleeps);
    for (int attempt = 1; attempt <= 4; attempt++) {
      IOException actual = assertThrows(IOException.class, () -> fetchHosted(client));
      assertEquals("One host admission must make one HTTP attempt", attempt, calls.get());
      assertSame(original, actual);
      assertTrue("Hosted attempts must never sleep inside the provider", sleeps.isEmpty());
    }
    IOException deferred = assertThrows(IOException.class, () -> fetchHosted(client));
    assertTrue(deferred instanceof P2pSnapshotClient.RetryLaterException);
    assertTrue(admissionDeferred((P2pSnapshotClient.RetryLaterException) deferred));
    assertEquals(4, calls.get()); // Unknown outcomes still consume both conservative windows.
    assertEquals(4_600_000L, ((P2pSnapshotClient.RetryLaterException) deferred).getRetryAtMillis());
    assertTrue(sleeps.isEmpty());
  }

  @Test public void hostedKnown200ReadFailureRetainsOriginalCauseAndCountedReservation() throws Exception {
    AtomicLong clock = new AtomicLong(1_000_000L);
    AtomicInteger calls = new AtomicInteger();
    List<Long> sleeps = new ArrayList<>();
    SocketException original = new SocketException("controlled counted body failure");
    java.lang.reflect.Constructor<P2pSnapshotClient.Response> constructor =
        P2pSnapshotClient.Response.class.getDeclaredConstructor(
            int.class, Map.class, byte[].class, IOException.class);
    constructor.setAccessible(true);
    P2pSnapshotClient.Response broken = constructor.newInstance(200,
        Collections.singletonMap("X-Snapshot-Height", "100"), new byte[0], original);
    P2pSnapshotClient client = client((uri, limit) -> { calls.incrementAndGet(); return broken; }, clock, sleeps);
    for (int attempt = 1; attempt <= 4; attempt++) {
      IOException actual = assertThrows(IOException.class, () -> fetchHosted(client));
      assertEquals("Known 200 body failure is still exactly one attempt", attempt, calls.get());
      assertSame(original, actual);
    }
    P2pSnapshotClient.RetryLaterException deferred = assertThrows(
        P2pSnapshotClient.RetryLaterException.class, () -> fetchHosted(client));
    assertTrue(admissionDeferred(deferred));
    assertEquals(4, calls.get());
    assertEquals(4_600_000L, deferred.getRetryAtMillis());
    assertTrue(sleeps.isEmpty());
  }

  @Test public void hosted429RetainsFullDeadlineAndSubsequentAdmissionDoesNotSend() throws Exception {
    long start = 1_000_000L;
    long deadline = start + 8_672_000L;
    AtomicLong clock = new AtomicLong(start);
    AtomicInteger calls = new AtomicInteger();
    List<Long> sleeps = new ArrayList<>();
    P2pSnapshotClient client = client((uri, limit) -> calls.incrementAndGet() == 1
        ? response(429, "Retry-After", "8672", new byte[0]) : success(), clock, sleeps);
    P2pSnapshotClient.RetryLaterException responseFailure = assertThrows(
        P2pSnapshotClient.RetryLaterException.class, () -> fetchHosted(client));
    assertEquals(1, calls.get());
    assertEquals(deadline, responseFailure.getRetryAtMillis());
    assertFalse("The actual 429 consumes one failed attempt", admissionDeferred(responseFailure));
    for (long now : new long[] {start, deadline - 1}) {
      clock.set(now);
      P2pSnapshotClient.RetryLaterException admission = assertThrows(
          P2pSnapshotClient.RetryLaterException.class, () -> fetchHosted(client));
      assertTrue("Waiting for admission is not a failed transport", admissionDeferred(admission));
      assertEquals(deadline, admission.getRetryAtMillis());
      assertEquals(1, calls.get());
      assertTrue(sleeps.isEmpty());
    }
    clock.set(deadline);
    assertEquals(BigInteger.valueOf(100), fetchHosted(client).height);
    assertEquals(2, calls.get());
    assertTrue(sleeps.isEmpty());
  }

  @Test public void hostedLocalAdmissionReturnsWithoutSleepingOrIssuingARequest() throws Exception {
    AtomicLong clock = new AtomicLong(1_000_000L);
    AtomicInteger calls = new AtomicInteger();
    List<Long> sleeps = new ArrayList<>();
    P2pSnapshotClient client = client((uri, limit) -> { calls.incrementAndGet(); return success(); }, clock, sleeps);
    for (int i = 0; i < 4; i++) fetchHosted(client);
    P2pSnapshotClient.RetryLaterException deferred = assertThrows(
        P2pSnapshotClient.RetryLaterException.class, () -> fetchHosted(client));
    assertTrue(admissionDeferred(deferred));
    assertEquals(4_600_000L, deferred.getRetryAtMillis());
    assertEquals(4, calls.get());
    assertEquals(1_000_000L, clock.get());
    assertTrue(sleeps.isEmpty());
  }

  @Test public void hostedBusyResponsesKeepEndpointMinimumWithoutNestedRetries() throws Exception {
    for (int status : new int[] {409, 502, 503}) {
      AtomicLong clock = new AtomicLong(1_000_000L);
      AtomicInteger calls = new AtomicInteger();
      List<Long> sleeps = new ArrayList<>();
      P2pSnapshotClient client = client((uri, limit) -> {
        calls.incrementAndGet();
        return response(status, "Retry-After", "17", new byte[0]);
      }, clock, sleeps);
      IOException actual = assertThrows(IOException.class, () -> fetchHosted(client));
      assertEquals("One failed busy response per host attempt", 1, calls.get());
      assertTrue(actual instanceof P2pSnapshotClient.RetryLaterException);
      P2pSnapshotClient.RetryLaterException deferred = (P2pSnapshotClient.RetryLaterException) actual;
      assertFalse(admissionDeferred(deferred));
      assertEquals(1_000_000L + (status == 503 ? 17_000L : 10_000L), deferred.getRetryAtMillis());
      assertTrue(sleeps.isEmpty());
    }
    assertFalse("Existing two-argument exceptions retain response-failure semantics",
        admissionDeferred(new P2pSnapshotClient.RetryLaterException("existing caller", 123L)));
  }

  @Test public void exactEncodedFiltersAndCommonHeightAreRequired() throws Exception {
    List<URI> requests = new ArrayList<>();
    Map<String, List<P2pWireCodec.Change>> books = new HashMap<>();
    books.put("BTC", Collections.emptyList());
    books.put("k:ABC", Collections.emptyList());
    P2pSnapshotClient client = client((uri, limit) -> {
      requests.add(uri);
      return response(200, "X-Snapshot-Height", "101", SyntheticWire.snapshot(101, books));
    }, new AtomicLong(), new ArrayList<>());
    P2pWireCodec.Snapshot snapshot = client.fetch(new LinkedHashSet<>(Arrays.asList("BTC", "k:ABC")), BigInteger.valueOf(100));
    assertEquals(books.keySet(), snapshot.books.keySet());
    assertEquals("min_height=100&coins=BTC%2Ck%3AABC", requests.get(0).getRawQuery());
  }

  @Test public void rejectsMissingHeaderOlderHeaderMembershipMismatchAndUnsafeUri() {
    for (P2pSnapshotClient.Response invalid : Arrays.asList(
        response(200, "Other", "100", SyntheticWire.seedBytes(100)),
        response(200, "X-Snapshot-Height", "99", SyntheticWire.seedBytes(99)),
        response(200, "X-Snapshot-Height", "101", SyntheticWire.seedBytes(100)),
        response(200, "X-Snapshot-Height", "100", SyntheticWire.snapshot(100,
            Collections.singletonMap("ETH", Collections.emptyList()))))) {
      P2pSnapshotClient client = client((uri, limit) -> invalid, new AtomicLong(), new ArrayList<>());
      assertThrows(IOException.class, () -> client.fetch(Collections.singleton("BTC"), BigInteger.valueOf(100)));
    }
    for (String uri : Arrays.asList("https://user:secret@snap.example/a", "https://snap.example/a#x",
        "file:///tmp/snapshot", "https://snap.example:0/a", "https://snap.example/a?coins=ALL")) {
      assertThrows(IllegalArgumentException.class, () -> new P2pSnapshotClient(URI.create(uri),
          Collections.emptySet(), new P2pWireCodec()));
    }
  }

  @Test public void redirectsRequireApprovedOriginsNoDowngradeAndConsumeQuota() throws Exception {
    // Retained historical test name: successful destinations consume quota; 307 hops do not.
    List<URI> calls = new ArrayList<>();
    P2pSnapshotClient.Transport transport = (uri, limit) -> {
      calls.add(uri);
      if (uri.getHost().equals("snap.example")) return response(307, "Location", "https://blob.example/data", new byte[0]);
      return response(200, "X-Snapshot-Height", "100", SyntheticWire.seedBytes(100));
    };
    P2pSnapshotClient denied = client(transport, new AtomicLong(), new ArrayList<>());
    assertThrows(IOException.class, () -> denied.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
    assertEquals(1, calls.size());
    calls.clear();
    AtomicLong clock = new AtomicLong();
    List<Long> sleeps = new ArrayList<>();
    P2pSnapshotClient approved = new P2pSnapshotClient(ENDPOINT,
        Collections.singleton(URI.create("https://blob.example")), new P2pWireCodec(), transport,
        clock::get, delay -> { sleeps.add(delay); clock.addAndGet(delay); });
    approved.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    approved.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    assertEquals(4, calls.size());
    assertTrue(sleeps.isEmpty());
    approved.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    approved.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    assertEquals("307 hops are free; only the four 200 responses count", 8, calls.size());
    assertTrue(sleeps.isEmpty());
    approved.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    assertEquals(10, calls.size());
    assertEquals(Collections.singletonList(3_600_000L), sleeps);
    P2pSnapshotClient downgrade = new P2pSnapshotClient(ENDPOINT,
        Collections.singleton(URI.create("http://blob.example")), new P2pWireCodec(),
        (uri, limit) -> response(307, "Location", "http://blob.example/data", new byte[0]), () -> 0, ignored -> {});
    assertThrows(IOException.class, () -> downgrade.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
  }

  @Test public void documentedBackoffAndRetryAfterShareOneBudgetAndExemptBusyResponses() throws Exception {
    AtomicLong clock = new AtomicLong();
    AtomicInteger calls = new AtomicInteger();
    List<Long> sleeps = new ArrayList<>();
    List<Integer> codes = Arrays.asList(409, 503, 429, 200);
    P2pSnapshotClient client = client((uri, limit) -> {
      int attempt = calls.getAndIncrement();
      int code = attempt < codes.size() ? codes.get(attempt) : 200;
      return code == 200 ? response(200, "X-Snapshot-Height", "100", SyntheticWire.seedBytes(100))
          : response(code, "Retry-After", "7", new byte[0]);
    }, clock, sleeps);
    client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    assertEquals(Arrays.asList(10_000L, 20_000L, 20_000L), sleeps);
    // All three documented error statuses are free; only successful responses count.
    client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    assertEquals(7, calls.get());
    assertEquals(3, sleeps.size());
  }

  @Test public void socketFailuresUseExactlyThreeExponentialRetries() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    AtomicLong clock = new AtomicLong();
    List<Long> sleeps = new ArrayList<>();
    SocketException original = new SocketException("offline reset");
    P2pSnapshotClient client = client((uri, limit) -> {
      calls.incrementAndGet();
      throw original;
    }, clock, sleeps);
    IOException failure = assertThrows(IOException.class,
        () -> client.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
    assertEquals(4, calls.get());
    assertEquals(Arrays.asList(5_000L, 10_000L, 20_000L), sleeps);
    assertSame(original, failure.getCause());
    assertTrue(failure.getMessage().contains("3 retries"));
  }

  @Test public void busyResponsesExhaustThreeRetriesWithoutInventingHourlyQuotaFailure() {
    AtomicInteger calls = new AtomicInteger();
    List<Long> sleeps = new ArrayList<>();
    P2pSnapshotClient client = client((uri, limit) -> {
      calls.incrementAndGet();
      return response(503, "Other", "none", new byte[0]);
    }, new AtomicLong(), sleeps);
    IOException failure = assertThrows(IOException.class,
        () -> client.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
    assertEquals(4, calls.get());
    assertEquals(Arrays.asList(10_000L, 20_000L, 40_000L), sleeps);
    assertTrue(failure.getMessage().contains("3 retries"));
    assertTrue(failure.getCause().getMessage().contains("503"));
  }

  @Test public void documented502RecoversAndDoesNotConsumeHourlyAllowance() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    AtomicLong clock = new AtomicLong();
    List<Long> sleeps = new ArrayList<>();
    P2pSnapshotClient client = client((uri, limit) -> calls.incrementAndGet() == 1
        ? response(502, "Other", "none", new byte[0])
        : response(200, "X-Snapshot-Height", "100", SyntheticWire.seedBytes(100)), clock, sleeps);
    assertEquals(BigInteger.valueOf(100), client.fetch(Collections.singleton("BTC"), BigInteger.ZERO).height);
    assertEquals(2, calls.get());
    assertEquals(Collections.singletonList(10_000L), sleeps);
    for (int i = 0; i < 3; i++) client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    assertEquals(5, calls.get());
    assertEquals("502 must not consume the hourly snapshot allowance",
        Collections.singletonList(10_000L), sleeps);
  }

  @Test public void documented502ExhaustsTheExistingThreeRetryBudget() {
    AtomicInteger calls = new AtomicInteger();
    List<Long> sleeps = new ArrayList<>();
    P2pSnapshotClient client = client((uri, limit) -> {
      calls.incrementAndGet();
      return response(502, "Other", "none", new byte[0]);
    }, new AtomicLong(), sleeps);
    IOException failure = assertThrows(IOException.class,
        () -> client.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
    assertEquals(4, calls.get());
    assertEquals(Arrays.asList(10_000L, 20_000L, 40_000L), sleeps);
    assertTrue(failure.getMessage().contains("3 retries"));
    assertEquals("Snapshot HTTP status 502", failure.getCause().getMessage());
  }

  @Test public void retryLogIdentifiesHttpStatusAndDeadlineWithoutResponseBody() throws Exception {
    Logger logger = (Logger) LoggerFactory.getLogger(P2pSnapshotClient.class);
    Level previous = logger.getLevel();
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.WARN);
    try {
      for (int status : Arrays.asList(429, 503)) {
        appender.list.clear();
        AtomicInteger calls = new AtomicInteger();
        P2pSnapshotClient client = client((uri, limit) -> calls.incrementAndGet() == 1
            ? response(status, "Retry-After", "8672", new byte[] {83, 69, 67, 82, 69, 84})
            : response(200, "X-Snapshot-Height", "100", SyntheticWire.seedBytes(100)),
            new AtomicLong(1_000L), new ArrayList<>());
        client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
        assertEquals(1, appender.list.size());
        String message = appender.list.get(0).getFormattedMessage();
        assertTrue(message, message.contains("reason=Snapshot HTTP status " + status));
        assertTrue(message, message.contains("delay_ms=8672000"));
        assertTrue(message, message.contains("retry_at_ms=8673000"));
        assertFalse(message, message.contains("SECRET"));
      }
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(previous);
      appender.stop();
    }
  }

  @Test public void longProviderCooldownsAreHonouredFor429And503() throws Exception {
    for (int code : Arrays.asList(429, 503)) {
      AtomicInteger calls = new AtomicInteger();
      AtomicLong clock = new AtomicLong();
      List<Long> sleeps = new ArrayList<>();
      P2pSnapshotClient client = client((uri, limit) -> calls.incrementAndGet() == 1
          ? response(code, "Retry-After", "120", new byte[0])
          : response(200, "X-Snapshot-Height", "100", SyntheticWire.seedBytes(100)), clock, sleeps);
      client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
      assertEquals(2, calls.get());
      assertEquals(Collections.singletonList(120_000L), sleeps);
      assertEquals(120_000L, clock.get());
    }
  }

  @Test public void localHourlyQuotaWaitsUntilTheActualReservationExpires() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    AtomicLong clock = new AtomicLong();
    List<Long> sleeps = new ArrayList<>();
    P2pSnapshotClient client = client((uri, limit) -> {
      calls.incrementAndGet();
      return response(200, "X-Snapshot-Height", "100", SyntheticWire.seedBytes(100));
    }, clock, sleeps);
    for (int i = 0; i < 5; i++) client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    assertEquals(5, calls.get());
    assertEquals(Collections.singletonList(3_600_000L), sleeps);
    assertEquals(3_600_000L, clock.get());
  }

  @Test public void busyHourlyExemptionDoesNotRemoveTheConservativeDailyLimit() throws Exception {
    // The daily cap still applies to counted responses, not to documented free errors.
    assertCountedAllowance((uri, limit) -> success(), true);
  }

  @Test public void documentedFreeResponsesConsumeNeitherHourlyNorDailyAllowance() throws Exception {
    List<P2pSnapshotClient.Response> free = Arrays.asList(
        error(403, "forbidden"), error(409, "not_ready"), error(429, "rate_limited"),
        error(502, "bad_gateway"), error(503, "busy"),
        error(400, "missing_min_height"), error(400, "invalid_min_height"),
        response(307, "Location", "/free", new byte[0]));
    for (P2pSnapshotClient.Response outcome : free) {
      AtomicLong clock = new AtomicLong(123_456L);
      AtomicInteger perFetch = new AtomicInteger();
      AtomicInteger calls = new AtomicInteger();
      AtomicInteger successful = new AtomicInteger();
      List<Long> sleeps = new ArrayList<>();
      P2pSnapshotClient client = client((uri, limit) -> {
        calls.incrementAndGet();
        if (successful.get() != 0) return success();
        // End a retry/redirect with another free response, never a counted success.
        return perFetch.getAndIncrement() == 0 ? outcome : error(403, "forbidden");
      }, clock, sleeps);
      for (int i = 0; i < 20; i++) {
        perFetch.set(0);
        assertThrows(IOException.class, () -> client.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
      }
      assertTrue("must exercise more than a day's allowance: " + outcome.status, calls.get() >= 20);
      assertTrue("free responses must not cause local quota waits: " + outcome.status,
          clock.get() < 123_456L + 3_600_000L);
      List<Long> backoffs = new ArrayList<>(sleeps);
      successful.set(1);
      long firstCountedAt = clock.get();
      for (int i = 0; i < 4; i++) client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
      assertEquals("free responses must leave all four hourly slots: " + outcome.status, backoffs, sleeps);
      client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
      assertEquals(firstCountedAt + 3_600_000L, clock.get());
    }
  }

  @Test public void countedAndUnclassified400ResponsesRetainBothRollingCaps() throws Exception {
    for (String code : Arrays.asList("invalid_query", "unknown_coin", "future_unknown_code")) {
      assertCountedAllowance((uri, limit) -> error(400, code), false);
    }
    assertCountedAllowance((uri, limit) -> response(400, "Other", "none", new byte[0]), false);
    assertCountedAllowance((uri, limit) -> response(400, "Other", "none",
        "not json".getBytes(StandardCharsets.UTF_8)), false);
  }

  @Test public void invalid200BodyStillConsumesBothRollingCaps() throws Exception {
    assertCountedAllowance((uri, limit) -> response(200, "X-Snapshot-Height", "100", new byte[0]), false);
  }

  @Test public void unknownTransportOutcomeRetainsBothRollingCaps() throws Exception {
    IOException unknown = new IOException("unknown delivery outcome");
    assertCountedAllowance((uri, limit) -> { throw unknown; }, false);
  }

  @Test public void admissionWaitDoesNotSpendAnyOfTheThreeHttpRetries() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    AtomicLong clock = new AtomicLong();
    List<Long> sleeps = new ArrayList<>();
    P2pSnapshotClient client = client((uri, limit) -> {
      int attempt = calls.incrementAndGet();
      return attempt <= 4 || attempt == 8 ? success() : error(503, "busy");
    }, clock, sleeps);
    for (int i = 0; i < 4; i++) client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    assertEquals(BigInteger.valueOf(100), client.fetch(Collections.singleton("BTC"), BigInteger.ZERO).height);
    assertEquals(8, calls.get());
    assertEquals(Arrays.asList(3_600_000L, 10_000L, 20_000L, 40_000L), sleeps);
  }

  @Test public void overlappingSameTimestampReservationsSettleOnceWithoutReleasingCountedAttempts()
      throws Exception {
    AtomicLong clock = new AtomicLong(123_456L);
    AtomicInteger calls = new AtomicInteger();
    AtomicReference<P2pSnapshotClient> owner = new AtomicReference<>();
    List<Long> sleeps = new ArrayList<>();
    P2pSnapshotClient.Response free = error(400, "invalid_min_height");
    P2pSnapshotClient client = client((uri, limit) -> {
      int attempt = calls.incrementAndGet();
      if (attempt == 1) {
        // Reentrant injected transport deterministically overlaps reservations at one timestamp.
        // Reusing the same response object must still settle each distinct attempt exactly once.
        for (int i = 0; i < 2; i++) assertThrows(IOException.class,
            () -> owner.get().fetch(Collections.singleton("BTC"), BigInteger.ZERO));
        return response(200, "X-Snapshot-Height", "100", new byte[0]);
      }
      return attempt <= 3 ? free : success();
    }, clock, sleeps);
    owner.set(client);
    assertThrows(IOException.class, () -> client.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
    for (int i = 0; i < 3; i++) client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    assertEquals(6, calls.get());
    assertTrue("two free settlements must not charge or erase the counted invalid 200", sleeps.isEmpty());
    client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    assertEquals(7, calls.get());
    assertEquals(Collections.singletonList(3_600_000L), sleeps);
    // Fifteen counted attempts total, plus the two free overlapping attempts.
    for (int i = 0; i < 10; i++) client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    assertEquals(17, calls.get());
    assertEquals(123_456L + 3 * 3_600_000L, clock.get());
    client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    assertEquals(18, calls.get());
    assertEquals("free settlement must release the daily slot exactly once too",
        123_456L + 86_400_000L, clock.get());
  }

  @Test public void known200ReadFailuresKeepStatusOriginalCauseAndThreeRetries() throws Exception {
    Logger logger = (Logger) LoggerFactory.getLogger(P2pSnapshotClient.class);
    Level previous = logger.getLevel();
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.INFO);
    try {
      for (IOException failure : Arrays.asList(new SocketException("SECRET_BODY_FAILURE"),
          new java.net.SocketTimeoutException("SECRET_BODY_FAILURE"),
          new java.io.EOFException("SECRET_BODY_FAILURE"))) {
        appender.list.clear();
        java.lang.reflect.Constructor<P2pSnapshotClient.Response> constructor =
            P2pSnapshotClient.Response.class.getDeclaredConstructor(
                int.class, Map.class, byte[].class, IOException.class);
        constructor.setAccessible(true);
        // Fault injection at the transport boundary: policy errors are separately exercised
        // through real HTTP. A successful status must survive transient body I/O failure too.
        P2pSnapshotClient.Response failed = constructor.newInstance(200,
            Collections.singletonMap("X-Snapshot-Height", "100"), new byte[0], failure);
        AtomicLong clock = new AtomicLong();
        AtomicInteger calls = new AtomicInteger();
        List<Long> waits = new ArrayList<>();
        P2pSnapshotClient client = client((uri, limit) -> calls.incrementAndGet() <= 4
            ? failed : success(), clock, waits);
        IOException exhausted = assertThrows(IOException.class,
            () -> client.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
        assertEquals("Snapshot retry budget exhausted after 3 retries", exhausted.getMessage());
        assertSame(failure, exhausted.getCause());
        assertEquals(4, calls.get());
        assertEquals(Arrays.asList(5000L, 10000L, 20000L), waits);
        List<ILoggingEvent> responses = events(appender, RESPONSE_LOG);
        assertEquals(4, responses.size());
        for (ILoggingEvent response : responses) {
          assertEquals("200", String.valueOf(response.getArgumentArray()[1]));
          assertEquals("counted", String.valueOf(response.getArgumentArray()[4]));
        }
        client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
        assertEquals(5, calls.get());
        assertEquals("all four body failures must still occupy hourly slots", 3_600_000L, clock.get());
        for (ILoggingEvent event : appender.list) assertFalse(event.getFormattedMessage().contains("SECRET"));
      }
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(previous);
      appender.stop();
    }
  }

  @Test public void missingOrInvalidHttpStatusRetainsConservativeReservations() throws Exception {
    for (int status : Arrays.asList(-1, 0, 600)) {
      assertCountedAllowance((uri, limit) -> response(status, null, null, new byte[0]), false);
    }
  }

  @Test public void finalExhausted429StillEmbargoesTheNextFetch() throws Exception {
    AtomicLong clock = new AtomicLong(1000);
    AtomicInteger calls = new AtomicInteger();
    List<Long> waits = new ArrayList<>();
    P2pSnapshotClient client = client((uri, limit) -> calls.incrementAndGet() <= 4
        ? response(429, "Retry-After", "120", new byte[0]) : success(), clock, waits);
    IOException exhausted = assertThrows(IOException.class,
        () -> client.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
    assertEquals("Snapshot retry budget exhausted after 3 retries", exhausted.getMessage());
    assertEquals("Snapshot HTTP status 429", exhausted.getCause().getMessage());
    assertEquals(4, calls.get());
    assertEquals(361_000L, clock.get());
    client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    assertEquals(5, calls.get());
    assertEquals(481_000L, clock.get());
    assertEquals(Arrays.asList(120_000L, 120_000L, 120_000L, 120_000L), waits);
  }

  @Test public void interruptedResponseRetainsKnown429BeforeCancellation() throws Exception {
    AtomicLong clock = new AtomicLong();
    AtomicInteger calls = new AtomicInteger();
    List<Long> waits = new ArrayList<>();
    P2pSnapshotClient client = client((uri, limit) -> {
      if (calls.incrementAndGet() == 1) {
        Thread.currentThread().interrupt();
        return response(429, "Retry-After", "8672", new byte[0]);
      }
      return success();
    }, clock, waits);
    try {
      assertThrows(InterruptedException.class,
          () -> client.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
      assertTrue(Thread.currentThread().isInterrupted());
      assertEquals(1, calls.get());
      assertTrue(waits.isEmpty());
    } finally { Thread.interrupted(); }
    client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    assertEquals(2, calls.get());
    assertEquals(Collections.singletonList(8_672_000L), waits);
  }

  @Test public void interruptionRetainsProviderCooldownAcrossFetchCalls() throws Exception {
    AtomicLong clock = new AtomicLong();
    AtomicInteger calls = new AtomicInteger();
    AtomicInteger waits = new AtomicInteger();
    List<Long> sleeps = new ArrayList<>();
    P2pSnapshotClient client = new P2pSnapshotClient(ENDPOINT, Collections.emptySet(),
        new P2pWireCodec(), (uri, limit) -> calls.incrementAndGet() == 1
            ? response(429, "Retry-After", "120", new byte[0])
            : response(200, "X-Snapshot-Height", "100", SyntheticWire.seedBytes(100)),
        clock::get, delay -> {
          sleeps.add(delay);
          if (waits.incrementAndGet() == 1) throw new InterruptedException("cancelled generation");
          clock.addAndGet(delay);
        });
    assertThrows(InterruptedException.class,
        () -> client.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
    assertEquals(1, calls.get());
    assertEquals(0, clock.get());
    client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    assertEquals(Arrays.asList(120_000L, 120_000L), sleeps);
    assertEquals(2, calls.get());
    assertEquals(120_000L, clock.get());
  }

  @Test public void malformedRetryAfterAndNonTransientTransportFailuresAreNotRetried() {
    for (int code : Arrays.asList(429, 503)) {
      AtomicInteger calls = new AtomicInteger();
      List<Long> sleeps = new ArrayList<>();
      P2pSnapshotClient client = client((uri, limit) -> {
        calls.incrementAndGet();
        return response(code, "Retry-After", "not a delay", new byte[0]);
      }, new AtomicLong(), sleeps);
      assertThrows(IOException.class, () -> client.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
      assertEquals(1, calls.get());
      assertTrue(sleeps.isEmpty());
    }
    AtomicInteger calls = new AtomicInteger();
    List<Long> sleeps = new ArrayList<>();
    IOException original = new IOException("Snapshot body limit");
    P2pSnapshotClient client = client((uri, limit) -> { calls.incrementAndGet(); throw original; },
        new AtomicLong(), sleeps);
    assertSame(original, assertThrows(IOException.class,
        () -> client.fetch(Collections.singleton("BTC"), BigInteger.ZERO)));
    assertEquals(1, calls.get());
    assertTrue(sleeps.isEmpty());
  }

  @Test public void redirectCyclesAndOversizedSnapshotBodiesStop() {
    P2pSnapshotClient cyclic = client((uri, limit) -> response(307, "Location", "/snapshot", new byte[0]),
        new AtomicLong(), new ArrayList<>());
    assertThrows(IOException.class, () -> cyclic.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
    P2pSnapshotClient bounded = new P2pSnapshotClient(ENDPOINT, Collections.emptySet(),
        new P2pWireCodec(64, 128, 2), (uri, limit) -> response(200, "X-Snapshot-Height", "100", new byte[129]),
        () -> 0, ignored -> {});
    assertThrows(IOException.class, () -> bounded.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
  }

  @Test public void infoLogsCorrelateOnlyActualAttemptsAcrossAdmissionRetryRedirectAndRepair()
      throws Exception {
    Logger logger = (Logger) LoggerFactory.getLogger(P2pSnapshotClient.class);
    Level previous = logger.getLevel();
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.INFO);
    try {
      AtomicLong clock = new AtomicLong();
      List<URI> calls = new ArrayList<>();
      List<Long> sleeps = new ArrayList<>();
      P2pSnapshotClient client = new P2pSnapshotClient(
          URI.create("https://snap.example/snapshot?auth=SECRET_QUERY"),
          Collections.singleton(URI.create("https://blob.example")), new P2pWireCodec(),
          (uri, limit) -> {
            calls.add(uri);
            assertEquals("request INFO must occur at the actual invocation, not at admission",
                calls.size(), events(appender, REQUEST_LOG).size());
            if (calls.size() == 5) return response(429, "Retry-After", "8672",
                "{\"window\":\"day\",\"code\":\"SECRET_CODE\",\"message\":\"SECRET_BODY\"}"
                    .getBytes(StandardCharsets.UTF_8));
            if (calls.size() == 6) return response(307, "Location",
                "https://blob.example/data?token=SECRET_REDIRECT", new byte[0]);
            return success();
          }, clock::get, delay -> {
            // Waiting must never emit a speculative network-attempt record.
            assertEquals(calls.size(), events(appender, REQUEST_LOG).size());
            sleeps.add(delay);
            clock.addAndGet(delay);
          });
      for (int i = 0; i < 5; i++) client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
      client.fetch(Collections.singleton("BTC"), BigInteger.valueOf(100));
      assertEquals(8, calls.size());
      assertEquals(Arrays.asList(3_600_000L, 8_672_000L), sleeps);
      List<ILoggingEvent> requests = events(appender, REQUEST_LOG);
      List<ILoggingEvent> responses = events(appender, RESPONSE_LOG);
      assertEquals(calls.size(), requests.size());
      assertEquals(calls.size(), responses.size());
      List<String> reasons = Arrays.asList("bootstrap", "bootstrap", "bootstrap", "bootstrap",
          "bootstrap", "retry", "redirect", "repair");
      LinkedHashSet<String> ids = new LinkedHashSet<>();
      for (int i = 0; i < requests.size(); i++) {
        Object[] request = requests.get(i).getArgumentArray();
        Object[] result = responses.get(i).getArgumentArray();
        String id = String.valueOf(request[0]);
        assertTrue("each actual attempt needs a unique bounded ID", !id.isEmpty() && id.length() <= 128 && ids.add(id));
        assertEquals(id, String.valueOf(result[0]));
        assertEquals(reasons.get(i), String.valueOf(request[1]));
        assertEquals("1", String.valueOf(request[2]));
        assertEquals(i == 7 ? "100" : "0", String.valueOf(request[3]));
        URI origin = URI.create(String.valueOf(request[4]));
        assertEquals("https", origin.getScheme());
        assertEquals(calls.get(i).getHost(), origin.getHost());
        assertNull(origin.getRawUserInfo());
        assertNull(origin.getRawQuery());
        assertNull(origin.getRawFragment());
        assertEquals(calls.get(i).getRawPath(), String.valueOf(request[5]));
        assertEquals(String.valueOf(i == 4 ? 429 : i == 5 ? 307 : 200), String.valueOf(result[1]));
        assertEquals(i == 4 || i == 5 ? "free" : "counted", String.valueOf(result[4]));
      }
      assertEquals("day", String.valueOf(responses.get(4).getArgumentArray()[3]));
      for (ILoggingEvent event : appender.list) {
        String message = event.getFormattedMessage();
        assertFalse(message, message.contains("SECRET"));
        assertFalse(message, message.contains("coins="));
      }
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(previous);
      appender.stop();
    }
  }

  @Test public void responseInfoLogsUseOnlyAllowlistedErrorCodesAndWindows() throws Exception {
    Logger logger = (Logger) LoggerFactory.getLogger(P2pSnapshotClient.class);
    Level previous = logger.getLevel();
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.INFO);
    try {
      for (String code : Arrays.asList("invalid_query", "unknown_coin", "missing_min_height",
          "invalid_min_height", "SECRET_CODE")) {
        appender.list.clear();
        P2pSnapshotClient client = client((uri, limit) -> error(400, code),
            new AtomicLong(), new ArrayList<>());
        assertThrows(IOException.class, () -> client.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
        List<ILoggingEvent> responses = events(appender, RESPONSE_LOG);
        assertEquals(1, responses.size());
        Object[] fields = responses.get(0).getArgumentArray();
        assertEquals("400", String.valueOf(fields[1]));
        if (!code.startsWith("SECRET")) assertEquals(code, String.valueOf(fields[2]));
        assertEquals(code.startsWith("SECRET") ? "unknown"
            : code.equals("invalid_query") || code.equals("unknown_coin") ? "counted" : "free",
            String.valueOf(fields[4]));
        for (ILoggingEvent event : appender.list) assertFalse(event.getFormattedMessage().contains("SECRET"));
      }
      for (String window : Arrays.asList("hour", "day", "SECRET_WINDOW")) {
        appender.list.clear();
        AtomicInteger calls = new AtomicInteger();
        P2pSnapshotClient client = client((uri, limit) -> calls.incrementAndGet() == 1
            ? response(429, "Retry-After", "0", ("{\"window\":\"" + window
                + "\",\"message\":\"SECRET_BODY\"}").getBytes(StandardCharsets.UTF_8)) : success(),
            new AtomicLong(), new ArrayList<>());
        client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
        List<ILoggingEvent> responses = events(appender, RESPONSE_LOG);
        assertEquals(2, responses.size());
        Object[] fields = responses.get(0).getArgumentArray();
        assertEquals("429", String.valueOf(fields[1]));
        assertEquals("free", String.valueOf(fields[4]));
        if (!window.startsWith("SECRET")) assertEquals(window, String.valueOf(fields[3]));
        for (ILoggingEvent event : appender.list) assertFalse(event.getFormattedMessage().contains("SECRET"));
      }
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(previous);
      appender.stop();
    }
  }

  @Test public void unsafe400MetadataIsUnknownAndConservativelyRetainsBothCaps() throws Exception {
    char[] padding = new char[4097];
    Arrays.fill(padding, 'x');
    String oversized = "{\"code\":\"missing_min_height\",\"message\":\"" + new String(padding) + "\"}";
    assertTrue(oversized.getBytes(StandardCharsets.UTF_8).length > 4096);
    List<String> bodies = Arrays.asList(
        "{\"code\":\"missing_min_height\",}",
        "{\"code\":\"invalid_min_height\"",
        "{\"code\":\"invalid_query\",\"code\":\"missing_min_height\"}",
        "{\"code\":\"missing_min_height\",\"code\":\"invalid_query\"}",
        "{\"code\":\"missing_min_height\",\"code\":\"missing_min_height\"}",
        "{\"code\":\"missing_min_height\"} {}",
        "{\"code\":\"invalid_min_height\"} true",
        "{\"code\":\"missing_min_height\"} SECRET_TRAILING_TOKEN",
        "{\"code\":null}", "{\"code\":123}", "{\"code\":true}",
        "{\"code\":[\"missing_min_height\"]}",
        "{\"code\":{\"code\":\"missing_min_height\"}}",
        "{\"error\":{\"code\":\"missing_min_height\"}}",
        "[{\"code\":\"missing_min_height\"}]",
        "{\"message\":\"\\\"code\\\":\\\"missing_min_height\\\" SECRET_BODY\"}",
        oversized);
    Logger logger = (Logger) LoggerFactory.getLogger(P2pSnapshotClient.class);
    Level previous = logger.getLevel();
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.INFO);
    try {
      for (int i = 0; i < bodies.size(); i++) {
        appender.list.clear();
        byte[] body = bodies.get(i).getBytes(StandardCharsets.UTF_8);
        assertCountedAllowance((uri, limit) -> response(400, "Other", "none", body), false);
        List<ILoggingEvent> responses = events(appender, RESPONSE_LOG);
        assertEquals("one response per actual attempt for unsafe metadata case " + i, 16, responses.size());
        for (ILoggingEvent event : responses) {
          Object[] fields = event.getArgumentArray();
          assertEquals("400", String.valueOf(fields[1]));
          assertEquals("unsafe metadata case " + i, "unknown", String.valueOf(fields[4]));
          assertFalse("unsafe metadata must not yield an allowlisted code", Arrays.asList(
              "invalid_query", "unknown_coin", "missing_min_height", "invalid_min_height")
              .contains(String.valueOf(fields[2])));
        }
        for (ILoggingEvent event : appender.list) assertFalse(event.getFormattedMessage().contains("SECRET"));
      }
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(previous);
      appender.stop();
    }
  }

  @Test public void validTopLevelCodesRemainUsableDespiteUnrelatedNestedMetadata() throws Exception {
    Logger logger = (Logger) LoggerFactory.getLogger(P2pSnapshotClient.class);
    Level previous = logger.getLevel();
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.INFO);
    try {
      for (String code : Arrays.asList("invalid_query", "unknown_coin", "missing_min_height", "invalid_min_height")) {
        appender.list.clear();
        byte[] body = ("{\"details\":{\"code\":\"SECRET_NESTED_CODE\"},\"message\":\"SECRET_BODY\",\"code\":\""
            + code + "\"}").getBytes(StandardCharsets.UTF_8);
        boolean counted = code.equals("invalid_query") || code.equals("unknown_coin");
        if (counted) {
          assertCountedAllowance((uri, limit) -> response(400, "Other", "none", body), false);
        } else {
          AtomicLong clock = new AtomicLong();
          AtomicInteger calls = new AtomicInteger();
          List<Long> sleeps = new ArrayList<>();
          P2pSnapshotClient client = client((uri, limit) -> {
            calls.incrementAndGet();
            return response(400, "Other", "none", body);
          }, clock, sleeps);
          for (int i = 0; i < 20; i++) assertThrows(IOException.class,
              () -> client.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
          assertEquals(20, calls.get());
          assertTrue("valid free code must release both windows", sleeps.isEmpty());
          assertEquals(0L, clock.get());
        }
        List<ILoggingEvent> responses = events(appender, RESPONSE_LOG);
        assertEquals(counted ? 16 : 20, responses.size());
        for (ILoggingEvent event : responses) {
          Object[] fields = event.getArgumentArray();
          assertEquals(code, String.valueOf(fields[2]));
          assertEquals(counted ? "counted" : "free", String.valueOf(fields[4]));
        }
        for (ILoggingEvent event : appender.list) assertFalse(event.getFormattedMessage().contains("SECRET"));
      }
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(previous);
      appender.stop();
    }
  }

  @Test public void unknownTransportIOExceptionLogsOneCorrelatedUnknownResponseWithoutItsMessage()
      throws Exception {
    Logger logger = (Logger) LoggerFactory.getLogger(P2pSnapshotClient.class);
    Level previous = logger.getLevel();
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.INFO);
    try {
      AtomicInteger calls = new AtomicInteger();
      List<Long> sleeps = new ArrayList<>();
      IOException original = new IOException("SECRET_TRANSPORT_MESSAGE");
      P2pSnapshotClient client = client((uri, limit) -> {
        calls.incrementAndGet();
        assertEquals("request log must precede the real transport invocation", 1, events(appender, REQUEST_LOG).size());
        assertTrue(events(appender, RESPONSE_LOG).isEmpty());
        throw original;
      }, new AtomicLong(), sleeps);
      assertSame(original, assertThrows(IOException.class,
          () -> client.fetch(Collections.singleton("BTC"), BigInteger.ZERO)));
      assertEquals(1, calls.get());
      assertTrue(sleeps.isEmpty());
      List<ILoggingEvent> requests = events(appender, REQUEST_LOG);
      List<ILoggingEvent> responses = events(appender, RESPONSE_LOG);
      assertEquals(1, requests.size());
      assertEquals(1, responses.size());
      Object[] request = requests.get(0).getArgumentArray();
      Object[] result = responses.get(0).getArgumentArray();
      assertEquals(String.valueOf(request[0]), String.valueOf(result[0]));
      assertEquals("bootstrap", String.valueOf(request[1]));
      assertEquals("unknown", String.valueOf(result[1]));
      assertEquals("unknown", String.valueOf(result[4]));
      for (ILoggingEvent event : appender.list) {
        assertFalse(event.getFormattedMessage().contains("SECRET"));
        assertNull("an attached exception would disclose its message", event.getThrowableProxy());
      }
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(previous);
      appender.stop();
    }
  }

  @Test public void asymmetricOverlappingSettlementPreservesTheCountedHourAndDayExpiry()
      throws Exception {
    long start = 123_456L;
    AtomicLong clock = new AtomicLong(start);
    AtomicInteger calls = new AtomicInteger();
    AtomicReference<P2pSnapshotClient> owner = new AtomicReference<>();
    List<Long> sleeps = new ArrayList<>();
    P2pSnapshotClient client = client((uri, limit) -> {
      int attempt = calls.incrementAndGet();
      if (attempt == 1) {
        // Equal timestamps cannot expose a swapped identity through expiry alone.
        // A later free reservation must not displace this earlier counted reservation.
        clock.addAndGet(1_000L);
        assertThrows(IOException.class,
            () -> owner.get().fetch(Collections.singleton("BTC"), BigInteger.ZERO));
        return response(200, "X-Snapshot-Height", "100", new byte[0]);
      }
      return attempt == 2 ? error(400, "missing_min_height") : success();
    }, clock, sleeps);
    owner.set(client);
    assertThrows(IOException.class, () -> client.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
    for (int i = 0; i < 3; i++) client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    assertTrue(sleeps.isEmpty());
    client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    assertEquals(6, calls.get());
    assertEquals(Collections.singletonList(3_599_000L), sleeps);
    assertEquals(start + 3_600_000L, clock.get());
    for (int i = 0; i < 10; i++) client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    assertEquals(16, calls.get()); // Fifteen counted attempts and one free response.
    assertEquals(start + 3 * 3_600_000L + 1_000L, clock.get());
    client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    assertEquals(17, calls.get());
    assertEquals("the oldest counted reservation, not the free response, anchors daily expiry",
        start + 86_400_000L, clock.get());
  }

  /** Compile against the pre-change API and fail on behavior, not a missing overload. */
  private static P2pWireCodec.Snapshot fetchHosted(P2pSnapshotClient client) throws Exception {
    java.lang.reflect.Method fetch;
    try {
      fetch = P2pSnapshotClient.class.getDeclaredMethod("fetch", java.util.Set.class,
          BigInteger.class, boolean.class);
    } catch (NoSuchMethodException beforeImplementation) {
      return client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    }
    fetch.setAccessible(true);
    try {
      return (P2pWireCodec.Snapshot) fetch.invoke(client, Collections.singleton("BTC"), BigInteger.ZERO, false);
    } catch (java.lang.reflect.InvocationTargetException failure) {
      Throwable cause = failure.getCause();
      if (cause instanceof Exception) throw (Exception) cause;
      if (cause instanceof Error) throw (Error) cause;
      throw new AssertionError(cause);
    }
  }

  private static boolean admissionDeferred(P2pSnapshotClient.RetryLaterException failure) throws Exception {
    try {
      return (Boolean) failure.getClass().getMethod("isAdmissionDeferred").invoke(failure);
    } catch (NoSuchMethodException beforeImplementation) {
      return false;
    }
  }

  private static List<ILoggingEvent> events(ListAppender<ILoggingEvent> appender, String template) {
    List<ILoggingEvent> found = new ArrayList<>();
    for (ILoggingEvent event : appender.list) {
      if (event.getLevel() == Level.INFO && template.equals(event.getMessage())) found.add(event);
    }
    return found;
  }

  private static void assertCountedAllowance(P2pSnapshotClient.Transport outcome, boolean succeeds)
      throws Exception {
    long start = 123_456L; // Deliberately not aligned to an epoch hour/day rollover.
    AtomicLong clock = new AtomicLong(start);
    List<Long> calls = new ArrayList<>();
    List<Long> sleeps = new ArrayList<>();
    P2pSnapshotClient client = client((uri, limit) -> {
      calls.add(clock.get());
      return outcome.get(uri, limit);
    }, clock, sleeps);
    for (int i = 0; i < 16; i++) {
      if (succeeds) client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
      else assertThrows(IOException.class, () -> client.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
      assertEquals("one network invocation per counted terminal outcome", i + 1, calls.size());
      assertEquals("conservative rolling hour/day admission for request " + (i + 1),
          start + (i < 15 ? (i / 4) * 3_600_000L : 86_400_000L), calls.get(i).longValue());
    }
    assertEquals(Arrays.asList(3_600_000L, 3_600_000L, 3_600_000L, 75_600_000L), sleeps);
  }

  private static P2pSnapshotClient.Response success() {
    return response(200, "X-Snapshot-Height", "100", SyntheticWire.seedBytes(100));
  }

  private static P2pSnapshotClient.Response error(int status, String code) {
    return response(status, "Retry-After", "0", ("{\"code\":\"" + code
        + "\",\"message\":\"SECRET_BODY\"}").getBytes(StandardCharsets.UTF_8));
  }

  private static P2pSnapshotClient client(P2pSnapshotClient.Transport transport, AtomicLong clock, List<Long> sleeps) {
    return new P2pSnapshotClient(ENDPOINT, Collections.emptySet(), new P2pWireCodec(), transport,
        clock::get, delay -> { sleeps.add(delay); clock.addAndGet(delay); });
  }
  private static P2pSnapshotClient.Response response(int code, String header, String value, byte[] bytes) {
    return new P2pSnapshotClient.Response(code, Collections.singletonMap(header, value), bytes);
  }
}
