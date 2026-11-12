package info.bitrich.xchangestream.hyperliquid.p2p;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Bounded snapshot HTTP boundary. Invoke on an I/O worker, never a Netty event loop. */
public final class P2pSnapshotClient {
  private static final Logger LOG = LoggerFactory.getLogger(P2pSnapshotClient.class);
  private static final int MAX_RETRIES = 3;
  private static final int MAX_ERROR_BYTES = 4096;
  private static final JsonFactory ERROR_JSON = new JsonFactory()
      .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
  private static final AtomicLong ATTEMPT_IDS = new AtomicLong();
  private final URI endpoint;
  private final Set<String> approvedOrigins;
  private final P2pWireCodec codec;
  private final Transport transport;
  private final LongSupplier clock;
  private final Sleeper sleeper;
  // Conservative instance-local rolling caps, not exact server counters or a shared-IP ledger.
  // Pending/unknown outcomes occupy both windows; documented free responses release both.
  private final Deque<Reservation> requests = new ArrayDeque<>();
  private final Deque<Reservation> hourlyRequests = new ArrayDeque<>();
  private long providerRetryAtMillis;

  public P2pSnapshotClient(URI endpoint, Set<URI> approvedOrigins, P2pWireCodec codec) {
    this(endpoint, approvedOrigins, codec, new HttpTransport(), System::currentTimeMillis,
        Thread::sleep);
  }

  P2pSnapshotClient(URI endpoint, Set<URI> origins, P2pWireCodec codec, Transport transport,
      LongSupplier clock, Sleeper sleeper) {
    this.endpoint = validateUri(endpoint);
    this.codec = codec;
    this.transport = transport;
    this.clock = clock;
    this.sleeper = sleeper;
    Set<String> normalized = new LinkedHashSet<>();
    if (origins != null) for (URI origin : origins) {
      validateUri(origin);
      if (origin.getRawQuery() != null || (origin.getPath() != null
          && !origin.getPath().isEmpty() && !"/".equals(origin.getPath()))) {
        throw new IllegalArgumentException("Redirect allowlist entries must be origins");
      }
      normalized.add(origin(origin));
    }
    this.approvedOrigins = Collections.unmodifiableSet(normalized);
    if (endpoint.getRawQuery() != null) {
      for (String parameter : endpoint.getRawQuery().split("&")) {
        String name = decodeQuery(parameter.split("=", 2)[0]);
        if ("coins".equals(name) || "min_height".equals(name)) {
          throw new IllegalArgumentException("Snapshot filter parameters are provider-owned");
        }
      }
    }
  }

  public P2pWireCodec.Snapshot fetch(Set<String> coins, BigInteger minHeight)
      throws IOException, InterruptedException {
    P2pWireCodec.unsigned(minHeight);
    if (coins == null || coins.isEmpty() || coins.size() > 512) {
      throw new IllegalArgumentException("Snapshot membership must contain 1..512 coins");
    }
    Set<String> membership = Collections.unmodifiableSet(new LinkedHashSet<>(coins));
    for (String coin : membership) {
      if (coin == null || coin.isEmpty() || coin.contains(",")) {
        throw new IllegalArgumentException("Invalid snapshot coin");
      }
    }
    String separator = endpoint.getRawQuery() == null ? "?" : "&";
    URI initial = URI.create(endpoint + separator + "min_height=" + minHeight + "&coins="
        + encodeQuery(String.join(",", membership)));
    URI current = initial;
    int hops = 0;
    int retries = 0;
    String reason = minHeight.signum() == 0 ? "bootstrap" : "repair";
    while (true) {
      if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Snapshot cancelled");
      Reservation reservation;
      try {
        reservation = reserveRequest();
      } catch (RetryLaterException deferred) {
        long delay = remainingDelay(deferred.getRetryAtMillis());
        LOG.info("P2P snapshot admission wait delay_ms={} reason={} retry_at_ms={}",
            delay, deferred.getMessage(), deferred.getRetryAtMillis());
        // Admission does not consume the HTTP retry budget and never logs a network attempt.
        sleeper.sleep(delay);
        continue;
      }
      if (Thread.currentThread().isInterrupted()) {
        settle(reservation, "free"); // No transport invocation took place.
        throw new InterruptedException("Snapshot cancelled");
      }
      Response response;
      LOG.info("P2P snapshot request attempt_id={} reason={} coin_count={} min_height={} origin={} path={}",
          reservation.id, reason, membership.size(), minHeight, origin(current), safePath(current));
      try {
        response = transport.get(current, codec.getMaxSnapshotBytes());
      } catch (IOException error) {
        settle(reservation, "unknown");
        LOG.info("P2P snapshot response attempt_id={} status={} code={} window={} accounting={}",
            reservation.id, "unknown", null, null, "unknown");
        retries = retryTransport(retries, error);
        current = initial;
        hops = 0;
        reason = "retry";
        continue;
      }
      ErrorMetadata metadata = errorMetadata(response);
      String accounting = accounting(response.status, metadata.code);
      settle(reservation, accounting);
      LOG.info("P2P snapshot response attempt_id={} status={} code={} window={} accounting={}",
          reservation.id, response.status, metadata.code, metadata.window, accounting);
      if (response.failure != null) {
        // A known 200 remains counted even if body I/O fails; preserve the original failure.
        retries = retryTransport(retries, response.failure);
        current = initial;
        hops = 0;
        reason = "retry";
        continue;
      }
      if (response.status == 200) {
        String value = response.header("X-Snapshot-Height");
        if (value == null || !value.matches("[0-9]+")) throw new IOException("Missing snapshot height");
        try {
          return codec.decodeSnapshot(response.body, membership, new BigInteger(value), minHeight);
        } catch (IllegalArgumentException e) {
          throw new IOException("Invalid snapshot", e);
        }
      }
      if (response.status == 307) {
        String location = response.header("Location");
        if (location == null || ++hops > 3) throw new IOException("Snapshot redirect limit/location");
        URI next;
        try { next = validateUri(current.resolve(location)); }
        catch (IllegalArgumentException e) { throw new IOException("Unsafe snapshot redirect", e); }
        if (("https".equalsIgnoreCase(current.getScheme())
                && !"https".equalsIgnoreCase(next.getScheme()))
            || (!origin(current).equals(origin(next)) && !approvedOrigins.contains(origin(next)))) {
          throw new IOException("Unapproved snapshot redirect");
        }
        current = next;
        reason = "redirect";
        continue;
      }
      if (response.status == 409 || response.status == 502
          || response.status == 503 || response.status == 429) {
        long delay = (response.status == 429 ? 5_000L : 10_000L) << retries;
        String retryAfter = response.header("Retry-After");
        if (response.status == 429 || (response.status == 503 && retryAfter != null)) {
          delay = Math.max(delay, retryAfter(retryAfter));
        }
        // Retain the embargo if a socket generation is cancelled and a later fetch takes over.
        long retryAt = deferProvider(delay);
        LOG.info("P2P snapshot cooldown attempt_id={} status={} retry_at_ms={}",
            reservation.id, response.status, retryAt);
        retries = retry(retries,
            new RetryLaterException("Snapshot HTTP status " + response.status, retryAt),
            remainingDelay(retryAt));
        current = initial;
        hops = 0;
        reason = "retry";
        continue;
      }
      throw new IOException("Snapshot HTTP status " + response.status);
    }
  }

  private int retryTransport(int retries, IOException error) throws IOException, InterruptedException {
    if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Snapshot cancelled");
    // Validation, TLS configuration and HTTP policy errors are not transient transport failures.
    if (!(error instanceof SocketException || error instanceof SocketTimeoutException
        || error instanceof UnknownHostException || error instanceof EOFException)) throw error;
    return retry(retries, error, 0);
  }

  private int retry(int retries, IOException failure, long requiredDelay)
      throws IOException, InterruptedException {
    if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Snapshot cancelled");
    if (retries >= MAX_RETRIES) {
      throw new IOException("Snapshot retry budget exhausted after 3 retries", failure);
    }
    long delay = Math.max(5_000L << retries, requiredDelay);
    // RetryLaterException messages are locally constructed status/quota reasons, not response bodies.
    LOG.warn("P2P snapshot retry attempt={}/{} delay_ms={} cause_class={} reason={} retry_at_ms={}",
        retries + 1, MAX_RETRIES, delay, failure.getClass().getName(),
        failure instanceof RetryLaterException ? failure.getMessage() : "transport",
        failure instanceof RetryLaterException ? ((RetryLaterException) failure).getRetryAtMillis() : null);
    // This is the existing cancellable snapshot worker, not a Netty/event/publication thread.
    sleeper.sleep(delay);
    return retries + 1;
  }

  private synchronized Reservation reserveRequest() throws IOException {
    long now = clock.getAsLong();
    while (!requests.isEmpty() && now - requests.peekFirst().atMillis >= 86_400_000L) requests.removeFirst();
    while (!hourlyRequests.isEmpty() && now - hourlyRequests.peekFirst().atMillis >= 3_600_000L) {
      hourlyRequests.removeFirst();
    }
    long next = providerRetryAtMillis;
    boolean quota = requests.size() >= 15 || hourlyRequests.size() >= 4;
    if (requests.size() >= 15) next = Math.max(next, retryDeadline(requests.peekFirst().atMillis, 86_400_000L));
    if (hourlyRequests.size() >= 4) {
      next = Math.max(next, retryDeadline(hourlyRequests.peekFirst().atMillis, 3_600_000L));
    }
    if (next > now) {
      throw new RetryLaterException(quota
          ? "Local snapshot quota exhausted (shared-IP quota may be lower)" : "Provider retry delay", next);
    }
    Reservation reservation = new Reservation(now);
    requests.addLast(reservation);
    hourlyRequests.addLast(reservation);
    return reservation;
  }

  private synchronized void settle(Reservation reservation, String accounting) {
    if (reservation.settled) return;
    reservation.settled = true;
    if ("free".equals(accounting)) {
      // Remove the attempt identity, never another request with the same clock timestamp.
      requests.remove(reservation);
      hourlyRequests.remove(reservation);
    }
  }

  private static final class Reservation {
    final long id = ATTEMPT_IDS.incrementAndGet();
    final long atMillis;
    boolean settled;
    Reservation(long atMillis) { this.atMillis = atMillis; }
  }

  private static String safePath(URI uri) {
    String path = uri.getRawPath();
    return path != null && path.matches("[/A-Za-z0-9._~-]{0,128}") ? path : "[redacted]";
  }

  private static String accounting(int status, String code) {
    if (status < 100 || status > 599) return "unknown";
    if (status == 200) return "counted";
    if (status == 400) {
      if ("invalid_query".equals(code) || "unknown_coin".equals(code)) return "counted";
      if ("missing_min_height".equals(code) || "invalid_min_height".equals(code)) return "free";
      return "unknown";
    }
    // The documented quota counts nothing else, including redirect responses.
    return "free";
  }

  private static ErrorMetadata errorMetadata(Response response) {
    if ((response.status != 400 && response.status != 429) || response.body == null
        || response.body.length == 0 || response.body.length > MAX_ERROR_BYTES) return ErrorMetadata.EMPTY;
    try (JsonParser parser = ERROR_JSON.createParser(response.body)) {
      if (parser.nextToken() != JsonToken.START_OBJECT) return ErrorMetadata.EMPTY;
      String code = null;
      String window = null;
      while (parser.nextToken() != JsonToken.END_OBJECT) {
        if (parser.currentToken() != JsonToken.FIELD_NAME) return ErrorMetadata.EMPTY;
        String field = parser.getCurrentName();
        JsonToken value = parser.nextToken();
        if (value == JsonToken.VALUE_STRING) {
          String text = parser.getText();
          if ("code".equals(field) && (response.status == 400
              ? "invalid_query".equals(text) || "unknown_coin".equals(text)
                  || "missing_min_height".equals(text) || "invalid_min_height".equals(text)
              : "rate_limited".equals(text))) code = text;
          if (response.status == 429 && "window".equals(field)
              && ("hour".equals(text) || "day".equals(text))) window = text;
        }
        parser.skipChildren();
      }
      if (parser.nextToken() != null) return ErrorMetadata.EMPTY;
      return new ErrorMetadata(code, window);
    } catch (IOException | RuntimeException ignored) {
      // Optional untrusted metadata must never hide status/Retry-After or leak into diagnostics.
      return ErrorMetadata.EMPTY;
    }
  }

  private static final class ErrorMetadata {
    static final ErrorMetadata EMPTY = new ErrorMetadata(null, null);
    final String code;
    final String window;
    ErrorMetadata(String code, String window) { this.code = code; this.window = window; }
  }

  private synchronized long deferProvider(long delay) throws IOException {
    providerRetryAtMillis = Math.max(providerRetryAtMillis, retryDeadline(clock.getAsLong(), delay));
    return providerRetryAtMillis;
  }

  private static long retryDeadline(long now, long delay) throws IOException {
    try { return Math.addExact(now, delay); }
    catch (ArithmeticException error) { throw new IOException("Snapshot retry deadline overflow", error); }
  }

  private long remainingDelay(long retryAt) throws IOException {
    try { return Math.max(0, Math.subtractExact(retryAt, clock.getAsLong())); }
    catch (ArithmeticException error) { throw new IOException("Snapshot retry delay overflow", error); }
  }

  private long retryAfter(String value) throws IOException {
    if (value == null) throw new IOException("429 requires Retry-After");
    try {
      if (value.matches("[0-9]+")) return Math.multiplyExact(Long.parseLong(value), 1000L);
      return Math.max(0, Math.subtractExact(ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
          .toInstant().toEpochMilli(), clock.getAsLong()));
    } catch (RuntimeException e) { throw new IOException("Invalid Retry-After", e); }
  }

  static URI validateUri(URI uri) {
    if (uri == null || !("https".equalsIgnoreCase(uri.getScheme())
        || "http".equalsIgnoreCase(uri.getScheme())) || uri.getHost() == null
        || uri.getRawUserInfo() != null || uri.getRawFragment() != null
        || uri.getPort() == 0 || uri.getPort() > 65535 || uri.getPort() < -1) {
      throw new IllegalArgumentException("Invalid snapshot URI");
    }
    return uri;
  }

  private static String encodeQuery(String text) {
    try { return URLEncoder.encode(text, "UTF-8").replace("+", "%20"); }
    catch (java.io.UnsupportedEncodingException impossible) { throw new AssertionError(impossible); }
  }

  private static String decodeQuery(String text) {
    try { return java.net.URLDecoder.decode(text, "UTF-8"); }
    catch (java.io.UnsupportedEncodingException impossible) { throw new AssertionError(impossible); }
  }

  private static String origin(URI uri) {
    int port = uri.getPort() < 0 ? ("https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80) : uri.getPort();
    return uri.getScheme().toLowerCase(java.util.Locale.ROOT) + "://"
        + uri.getHost().toLowerCase(java.util.Locale.ROOT) + ":" + port;
  }

  interface Transport { Response get(URI uri, int maxBytes) throws IOException; }
  interface Sleeper { void sleep(long milliseconds) throws InterruptedException; }

  static final class Response {
    final int status;
    final Map<String, String> headers;
    final byte[] body;
    final IOException failure;
    Response(int status, Map<String, String> headers, byte[] body) {
      this(status, headers, body, null);
    }
    private Response(int status, Map<String, String> headers, byte[] body, IOException failure) {
      this.status = status;
      this.headers = headers;
      this.body = body;
      this.failure = failure;
    }
    String header(String name) {
      return headers.entrySet().stream().filter(e -> name.equalsIgnoreCase(e.getKey()))
          .map(Map.Entry::getValue).findFirst().orElse(null);
    }
  }

  public static final class RetryLaterException extends IOException {
    private final long retryAtMillis;
    RetryLaterException(String message, long retryAtMillis) {
      super(message);
      this.retryAtMillis = retryAtMillis;
    }
    public long getRetryAtMillis() { return retryAtMillis; }
  }

  private static final class HttpTransport implements Transport {
    @Override public Response get(URI uri, int maxBytes) throws IOException {
      HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
      connection.setInstanceFollowRedirects(false);
      connection.setConnectTimeout(10_000);
      connection.setReadTimeout(30_000);
      connection.setRequestMethod("GET");
      connection.setRequestProperty("Accept", "application/octet-stream");
      connection.setRequestProperty("Accept-Encoding", "identity");
      try {
        int status = connection.getResponseCode();
        Map<String, String> headers = new java.util.HashMap<>();
        connection.getHeaderFields().forEach((name, values) -> {
          if (name != null && values != null && !values.isEmpty()) headers.put(name, values.get(0));
        });
        byte[] body = new byte[0];
        if (status == 200) {
          try {
            if (connection.getContentLengthLong() > maxBytes) throw new IOException("Snapshot body limit");
            String encoding = connection.getContentEncoding();
            if (encoding != null && !"identity".equalsIgnoreCase(encoding)) {
              throw new IOException("Unexpected snapshot compression");
            }
            try (InputStream in = connection.getInputStream();
                ByteArrayOutputStream out = new ByteArrayOutputStream()) {
              byte[] buffer = new byte[8192];
              int n;
              while ((n = in.read(buffer)) != -1) {
                if (Thread.currentThread().isInterrupted()) throw new IOException("Snapshot cancelled");
                if (n > maxBytes - out.size()) throw new IOException("Snapshot body limit");
                out.write(buffer, 0, n);
              }
              body = out.toByteArray();
            }
          } catch (IOException failure) {
            return new Response(status, headers, body, failure);
          }
        } else if (status == 400 || status == 429) {
          body = optionalErrorBody(connection);
        }
        return new Response(status, headers, body);
      } finally { connection.disconnect(); }
    }

    private static byte[] optionalErrorBody(HttpURLConnection connection) {
      byte[] absent = new byte[0];
      long length = connection.getContentLengthLong();
      // HttpURLConnection cannot reliably shorten an established socket's read timeout.
      // Only inspect a complete buffered, bounded, fixed-length plain-HTTP body. Chunked
      // framing and TLS completion can themselves block, so unsupported bodies stay unknown.
      if (!"http".equalsIgnoreCase(connection.getURL().getProtocol())
          || connection.getHeaderField("Transfer-Encoding") != null
          || (connection.getContentEncoding() != null
              && !"identity".equalsIgnoreCase(connection.getContentEncoding()))
          || length <= 0 || length > MAX_ERROR_BYTES) return absent;
      InputStream in = connection.getErrorStream();
      if (in == null) return absent;
      long start = System.nanoTime();
      try {
        while (!Thread.currentThread().isInterrupted()
            && System.nanoTime() - start < 250_000_000L) {
          // No framing read, readFully, EOF probe or draining close on an optional body.
          // InputStream guarantees one read of available() bytes will not block.
          if (in.available() >= length) {
            byte[] body = new byte[(int) length];
            return in.read(body) == length ? body : absent;
          }
          Thread.sleep(10);
        }
      } catch (InterruptedException stopped) {
        Thread.currentThread().interrupt();
      } catch (IOException ignored) {
        // Keep the already received status and authoritative Retry-After.
      }
      return absent;
    }
  }
}
