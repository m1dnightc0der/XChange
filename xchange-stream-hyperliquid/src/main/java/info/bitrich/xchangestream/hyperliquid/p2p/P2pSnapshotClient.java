package info.bitrich.xchangestream.hyperliquid.p2p;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.HttpURLConnection;
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

/** Bounded snapshot HTTP boundary. Invoke on an I/O worker, never a Netty event loop. */
public final class P2pSnapshotClient {
  private final URI endpoint;
  private final Set<String> approvedOrigins;
  private final P2pWireCodec codec;
  private final Transport transport;
  private final LongSupplier clock;
  private final Sleeper sleeper;
  private final Deque<Long> requests = new ArrayDeque<>();

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
    while (true) {
      if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
      reserveRequest();
      Response response = transport.get(current, codec.getMaxSnapshotBytes());
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
        continue;
      }
      if (response.status == 409 || response.status == 503 || response.status == 429) {
        if (retries >= 3) throw new IOException("Snapshot retry budget exhausted");
        long delay = response.status == 429 ? retryAfter(response.header("Retry-After"))
            : 10_000L << retries;
        retries++;
        if (delay > 60_000) throw new RetryLaterException("Provider retry delay", clock.getAsLong() + delay);
        sleeper.sleep(delay);
        current = initial;
        hops = 0;
        continue;
      }
      throw new IOException("Snapshot HTTP status " + response.status);
    }
  }

  private synchronized void reserveRequest() throws RetryLaterException {
    long now = clock.getAsLong();
    while (!requests.isEmpty() && now - requests.peekFirst() >= 86_400_000L) requests.removeFirst();
    long hourCount = requests.stream().filter(t -> now - t < 3_600_000L).count();
    if (requests.size() >= 15 || hourCount >= 4) {
      long next = requests.size() >= 15 ? requests.peekFirst() + 86_400_000L
          : requests.stream().filter(t -> now - t < 3_600_000L).findFirst().get() + 3_600_000L;
      throw new RetryLaterException("Local snapshot quota exhausted (shared-IP quota may be lower)", next);
    }
    requests.addLast(now);
  }

  private long retryAfter(String value) throws IOException {
    if (value == null) throw new IOException("429 requires Retry-After");
    try {
      if (value.matches("[0-9]+")) return Math.multiplyExact(Long.parseLong(value), 1000L);
      return Math.max(0, ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
          .toInstant().toEpochMilli() - clock.getAsLong());
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
    Response(int status, Map<String, String> headers, byte[] body) {
      this.status = status;
      this.headers = headers;
      this.body = body;
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
        }
        return new Response(status, headers, body);
      } finally { connection.disconnect(); }
    }
  }
}
