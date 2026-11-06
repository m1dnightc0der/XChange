package info.bitrich.xchangestream.hyperliquid.p2p;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.service.netty.NettyStreamingService;
import info.bitrich.xchangestream.service.netty.WebSocketClientHandler;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.extensions.WebSocketClientExtensionHandler;
import io.netty.handler.ssl.SslHandler;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import io.reactivex.rxjava3.subjects.CompletableSubject;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.SSLParameters;

/** Provider-local binary handler and ordered state machine on the existing Netty transport hooks. */
public class P2pStreamingService extends NettyStreamingService<String> {
  public enum Readiness { DISCONNECTED, SEEDING, CATCHING_UP, LIVE, FAILED }
  private enum Recovery { COLD, WARM, LIVE_FIRST }
  private static final long MAX_QUEUE_AGE_NANOS = TimeUnit.SECONDS.toNanos(5);
  private static final long MAX_INGRESS_SILENCE_NANOS = TimeUnit.SECONDS.toNanos(30);
  private static final long MAX_PENDING_BYTES = 32L * 1024 * 1024;
  private static final int MAX_PENDING_FRAMES = 4096;
  private final Set<String> coins;
  private final P2pWireCodec codec;
  private final SnapshotSource snapshotSource;
  private final ObjectMapper mapper = new ObjectMapper();
  private final ThreadPoolExecutor work;
  private final ExecutorService http = Executors.newSingleThreadExecutor(daemon("p2p-snapshot"));
  private final ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor(daemon("p2p-health"));
  private final Object stateGate = new Object();
  private final AtomicLong generation = new AtomicLong();
  private final AtomicLong queuedBytes = new AtomicLong();
  private final AtomicBoolean stopped = new AtomicBoolean();
  private final Subject<Readiness> readinessEvents = BehaviorSubject.<Readiness>createDefault(Readiness.DISCONNECTED).toSerialized();
  private final Subject<Publication> publications = BehaviorSubject.<Publication>create().toSerialized();
  private final Subject<Observation> observations = PublishSubject.<Observation>create().toSerialized();
  private volatile Readiness readiness = Readiness.DISCONNECTED;
  private volatile Cursor cursor;
  private volatile Throwable lastFailure;
  private volatile Recovery recovery = Recovery.COLD;
  private volatile Channel activeChannel;
  private volatile BinaryHandler activeHandler;
  private volatile Future<?> snapshotTask;
  private volatile long lastIngressNanos;
  private volatile long executingSinceNanos;
  private volatile long negotiationSinceNanos;
  private volatile boolean negotiated;
  private volatile CompletableSubject preparation;
  // All reconstruction fields below have exactly one writer: work.
  private long workerToken;
  private int totalOrders;
  private Stamp lastLiveStamp;
  private final Map<String, P2pOrderBook.View> lastViews = new LinkedHashMap<>();
  private Map<String, P2pOrderBook> booksByCoin = new LinkedHashMap<>();
  private BigInteger nextSequence = BigInteger.ONE;
  private long replayRemaining;
  private boolean seeded;
  private boolean subscribed;
  private boolean liveStatus;
  private BigInteger liveHeight;
  private BigInteger lastFrameHeight;
  private BigInteger snapshotCoveredHeight;
  private boolean snapshotPending;
  private final List<Decoded> pending = new ArrayList<>();
  private long pendingBytes;

  public P2pStreamingService(URI websocket, Set<String> coins, P2pSnapshotClient snapshots) {
    this(websocket, coins, new P2pWireCodec(), snapshots::fetch, 256);
  }

  P2pStreamingService(URI websocket, Set<String> coins, P2pWireCodec codec,
      SnapshotSource snapshotSource, int queueCapacity) {
    super(validateWebSocket(websocket).toString(), codec.getMaxFrameBytes(),
        Duration.ofSeconds(10), Duration.ofSeconds(5), 5);
    if (coins == null || coins.isEmpty() || coins.size() > 512 || queueCapacity < 1) {
      throw new IllegalArgumentException("P2P membership must contain 1..512 coins");
    }
    for (String coin : coins) {
      if (coin == null || coin.isEmpty() || coin.contains(",") || coin.length() > 255) {
        throw new IllegalArgumentException("Invalid coin");
      }
    }
    this.coins = Collections.unmodifiableSet(new LinkedHashSet<>(coins));
    this.codec = codec;
    this.snapshotSource = snapshotSource;
    this.work = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(queueCapacity), daemon("p2p-books"), new ThreadPoolExecutor.AbortPolicy());
    this.coins.forEach(coin -> booksByCoin.put(coin, new P2pOrderBook(coin)));
    watchdog.scheduleAtFixedRate(this::checkHealth, 1, 1, TimeUnit.SECONDS);
  }

  private static URI validateWebSocket(URI uri) {
    if (uri == null || !("ws".equalsIgnoreCase(uri.getScheme()) || "wss".equalsIgnoreCase(uri.getScheme()))
        || uri.getHost() == null || uri.getRawUserInfo() != null || uri.getRawFragment() != null
        || uri.getPort() == 0 || uri.getPort() > 65535 || uri.getPort() < -1) {
      throw new IllegalArgumentException("Invalid P2P WebSocket URI");
    }
    return uri;
  }

  private static ThreadFactory daemon(String name) {
    return task -> { Thread thread = new Thread(task, name); thread.setDaemon(true); return thread; };
  }

  public Readiness getReadiness() { return readiness; }
  public long getGeneration() { return generation.get(); }
  public Cursor getCursor() { return cursor; }
  public Set<String> getSubscribedCoins() { return coins; }
  public Throwable getLastFailure() { return lastFailure; }
  public Observable<Readiness> readiness() { return readinessEvents.filter(value -> value == readiness); }
  public Observable<Observation> observations() {
    return observations.filter(value -> value.generation == generation.get() && readiness == Readiness.LIVE);
  }

  public Observable<P2pOrderBook.View> book(String coin) {
    if (!coins.contains(coin)) return Observable.error(new IllegalArgumentException("Unsubscribed coin"));
    return publications.filter(p -> readiness == Readiness.LIVE && p.generation == generation.get())
        .map(p -> p.books.get(coin)).distinctUntilChanged();
  }

  /** Completion gate for exchange callers; the inherited reconnect owner only waits for transport. */
  public Completable awaitLive() {
    return readiness().filter(s -> s == Readiness.LIVE || s == Readiness.FAILED
        || (s == Readiness.DISCONNECTED && stopped.get())).firstOrError()
        .flatMapCompletable(s -> s == Readiness.LIVE ? Completable.complete()
            : Completable.error(lastFailure == null ? new IOException("P2P disconnected") : lastFailure));
  }

  @Override public Completable connect() {
    return Completable.defer(() -> {
      if (stopped.get() || readiness == Readiness.FAILED) {
        return Completable.error(new IllegalStateException("P2P service is stopped"));
      }
      CompletableSubject prepared = CompletableSubject.create();
      preparation = prepared;
      long token = generation.get();
      boolean accepted = enqueue(token, 0, () -> {
        try { prepare(prepared); }
        catch (Throwable error) { prepared.onError(error); fail(error); }
      }, System.nanoTime());
      if (!accepted) prepared.onError(new IOException("P2P connection preparation rejected"));
      return prepared.andThen(Completable.defer(() -> stopped.get() || readiness == Readiness.FAILED
          ? Completable.error(new IOException("P2P connection cancelled")) : super.connect())).doOnComplete(() -> {
        long current = generation.get();
        negotiationSinceNanos = System.nanoTime();
        lastIngressNanos = negotiationSinceNanos;
        sendControl("{\"method\":\"info\"}", current);
      });
    });
  }

  private void prepare(CompletableSubject prepared) {
    long token;
    synchronized (stateGate) { token = generation.incrementAndGet(); }
    workerToken = token;
    preparation = prepared;
    negotiated = false;
    subscribed = false;
    liveStatus = false;
    liveHeight = null;
    lastLiveStamp = null;
    lastFrameHeight = null;
    snapshotCoveredHeight = null;
    nextSequence = BigInteger.ONE;
    pending.clear();
    pendingBytes = 0;
    snapshotPending = false;
    negotiationSinceNanos = 0;
    if (recovery == Recovery.LIVE_FIRST) {
      seeded = false;
      cursor = null;
    }
    if (recovery == Recovery.COLD || (!seeded && recovery != Recovery.LIVE_FIRST)) {
      seeded = false;
      cursor = null;
      setReadiness(Readiness.SEEDING);
      requestSnapshot(token, BigInteger.ZERO, prepared);
    } else {
      replayRemaining = recovery == Recovery.WARM && cursor != null ? cursor.framesAtHeight : 0;
      setReadiness(seeded ? Readiness.CATCHING_UP : Readiness.SEEDING);
      preparation = null;
      prepared.onComplete();
    }
  }

  private void requestSnapshot(long token, BigInteger minHeight, CompletableSubject prepared) {
    snapshotPending = true;
    snapshotTask = http.submit(() -> {
      try {
        P2pWireCodec.Snapshot snapshot = snapshotSource.fetch(coins, minHeight);
        enqueue(token, 0, () -> {
          try {
          if (!snapshot.books.keySet().equals(coins) || snapshot.height.compareTo(minHeight) < 0) {
            throw new IllegalArgumentException("Snapshot membership/height mismatch");
          }
          Map<String, P2pOrderBook> replacement = new LinkedHashMap<>();
          snapshot.books.forEach((coin, orders) -> {
            P2pOrderBook book = new P2pOrderBook(coin);
            book.seed(orders);
            replacement.put(coin, book);
          });
          if (snapshot.height.equals(P2pWireCodec.MAX_U64)) throw new IllegalArgumentException("Height exhausted");
          if (token != generation.get() || stopped.get()) return;
          booksByCoin = replacement;
          totalOrders = replacement.values().stream().mapToInt(P2pOrderBook::getOrderCount).sum();
          lastViews.clear();
          cursor = new Cursor(snapshot.height.add(BigInteger.ONE), 0);
          seeded = true;
          snapshotPending = false;
          replayRemaining = 0;
          if (prepared != null) {
            setRecovery(Recovery.WARM);
            setReadiness(Readiness.CATCHING_UP);
            preparation = null;
            prepared.onComplete();
          } else {
            // HTTP can overtake WebSocket ingress; later frames at/below H are covered too.
            snapshotCoveredHeight = snapshot.height;
            for (Decoded buffered : pending) {
              if (buffered.diff.height.compareTo(snapshotCoveredHeight) > 0) apply(buffered);
            }
            pending.clear();
            pendingBytes = 0;
            setRecovery(Recovery.WARM);
            maybeLive(lastLiveStamp);
          }
          } catch (Throwable error) {
            if (token == generation.get()) {
              if (prepared != null) prepared.onError(error);
              fail(error);
            }
          }
        }, System.nanoTime());
      } catch (Throwable error) {
        if (token != generation.get() || stopped.get()) return;
        if (prepared != null) prepared.onError(error);
        enqueue(token, 0, () -> fail(error), System.nanoTime());
      }
    });
  }

  @Override public void messageHandler(String message) {
    // The provider handler calls this seam only for a complete, bounded UTF-8 control message.
    long token = generation.get();
    enqueue(token, message.length() * 2L, () -> control(message, token), System.nanoTime());
  }

  private void control(String message, long token) throws IOException {
    JsonNode root;
    try { root = mapper.readTree(message); }
    catch (IOException error) { fail(new IOException("Invalid P2P control JSON", error)); return; }
    if (root == null || !root.isObject()) { fail(new IOException("Invalid control message")); return; }
    String channel = root.path("channel").asText();
    String error = root.path("error").asText();
    if (error.isEmpty()) error = root.path("error").path("code").asText();
    if (error.isEmpty()) error = root.path("code").asText();
    if (error.isEmpty() && "error".equals(channel)) {
      JsonNode data = root.path("data");
      error = data.isTextual() ? data.asText() : data.path("code").asText();
    }
    if ("history_unavailable".equals(error)) {
      if (!subscribed) { fail(new IOException("History error before subscription")); return; }
      seeded = false;
      setRecovery(Recovery.LIVE_FIRST);
      invalidate(token, new IOException("history_unavailable"), false);
      return;
    }
    if (!error.isEmpty() || "error".equals(channel)) {
      fail(new IOException("P2P control error: " + error));
      return;
    }
    if (!"info".equals(channel)) return; // esp/prime/subscribe have no success acknowledgement.
    if (negotiated || !root.path("binary_version").isIntegralNumber()
        || root.path("binary_version").asInt() != 3
        || !"on".equals(root.path("streams").path("book").asText())) {
      fail(new IOException("P2P requires v3 and enabled book stream"));
      return;
    }
    negotiated = true;
    sendControl("{\"method\":\"esp\",\"version\":3}", token);
    Map<String, Object> subscribe = new LinkedHashMap<>();
    subscribe.put("method", "subscribe");
    subscribe.put("coins", coins);
    if (recovery != Recovery.LIVE_FIRST) subscribe.put("from_height", cursor.height);
    subscribed = true;
    sendControl(mapper.writeValueAsString(subscribe), token);
  }

  protected void sendControl(String message, long token) {
    if (token != generation.get() || stopped.get()) return;
    try {
      sendMessage(message).addListener(f -> {
        if (!f.isSuccess()) invalidate(token, f.cause(), false);
      });
    } catch (RuntimeException error) { invalidate(token, error, false); }
  }

  // Exact singleton subscription is sent after info, never by native automatic channel replay.
  @Override public void resubscribeChannels() {}
  @Override protected String getChannelNameFromMessage(String message) { return null; }
  @Override public String getSubscribeMessage(String channel, Object... args) {
    throw new UnsupportedOperationException("P2P membership is fixed at connect");
  }
  @Override public String getUnsubscribeMessage(String channel, Object... args) {
    throw new UnsupportedOperationException("P2P membership is immutable");
  }
  @Override protected WebSocketClientExtensionHandler getWebSocketClientExtensionHandler() { return null; }

  void binary(long token, byte[] bytes, long micros, long nanos) {
    enqueue(token, bytes.length, () -> {
      long decodeStartedNanos = System.nanoTime();
      process(codec.decode(bytes), bytes.length, micros, nanos, decodeStartedNanos);
    }, nanos);
  }

  private void process(P2pWireCodec.Frame frame, int bytes, long micros, long nanos, long decodeStartedNanos) {
    long decodedNanos = System.nanoTime();
    if (frame instanceof P2pWireCodec.Ignored) return;
    if (!negotiated || !subscribed) {
      fail(new IllegalArgumentException("Book traffic before negotiation"));
      return;
    }
    if (frame instanceof P2pWireCodec.Status) {
      P2pWireCodec.Status status = (P2pWireCodec.Status) frame;
      if (status.stream != 0) return;
      if (status.state == 2) {
        setRecovery(Recovery.COLD);
        seeded = false;
        invalidate(generation.get(), new IOException("Provider book gap"), true);
      } else if (status.state == 0) {
        liveStatus = false;
        setReadiness(seeded ? Readiness.CATCHING_UP : Readiness.SEEDING);
      } else {
        if (replayRemaining != 0) throw new IllegalArgumentException("Incomplete same-height replay");
        liveStatus = true;
        liveHeight = status.height;
        lastLiveStamp = new Stamp(micros, nanos);
        maybeLive(lastLiveStamp);
      }
      return;
    }
    P2pWireCodec.Diff diff = (P2pWireCodec.Diff) frame;
    if (!diff.sequence.equals(nextSequence)) {
      invalidate(generation.get(), new IOException("Book sequence discontinuity"), false);
      return;
    }
    nextSequence = nextSequence.add(BigInteger.ONE);
    if (lastFrameHeight != null && diff.height.compareTo(lastFrameHeight) < 0) {
      throw new IllegalArgumentException("Regressing book height");
    }
    lastFrameHeight = diff.height;
    for (P2pWireCodec.Change change : diff.changes) {
      if (!coins.contains(change.coin)) throw new IllegalArgumentException("Unexpected book coin");
    }
    Decoded decoded = new Decoded(diff, micros, nanos, decodeStartedNanos, decodedNanos, bytes);
    if (!seeded) {
      if (recovery != Recovery.LIVE_FIRST) throw new IllegalArgumentException("Unseeded diff");
      if (pending.size() >= MAX_PENDING_FRAMES || pendingBytes + bytes > MAX_PENDING_BYTES) {
        throw new IllegalArgumentException("Live-first buffer overflow");
      }
      pending.add(decoded);
      pendingBytes += bytes;
      if (!snapshotPending) requestSnapshot(generation.get(), diff.height, null);
      return;
    }
    // Sequence, height ordering and membership were checked even for covered frames.
    if (snapshotCoveredHeight != null && diff.height.compareTo(snapshotCoveredHeight) <= 0) return;
    if (replayRemaining > 0) {
      if (!diff.height.equals(cursor.height)) throw new IllegalArgumentException("Replay skipped cursor height");
      replayRemaining--;
      return;
    }
    apply(decoded);
    if (readiness == Readiness.LIVE) publish(decoded);
  }

  private void apply(Decoded decoded) {
    P2pWireCodec.Diff diff = decoded.diff;
    decoded.applyStartedNanos = System.nanoTime();
    if (diff.height.compareTo(cursor.height) < 0) throw new IllegalArgumentException("Frame before cursor");
    Set<P2pOrderBook> changedBooks = new LinkedHashSet<>();
    for (P2pWireCodec.Change change : diff.changes) {
      P2pOrderBook book = booksByCoin.get(change.coin);
      int before = book.getOrderCount();
      book.apply(change);
      changedBooks.add(book);
      totalOrders += book.getOrderCount() - before;
      if (totalOrders > 2_000_000) throw new IllegalArgumentException("Runtime order limit");
    }
    // A frame may temporarily cross while replacing levels; only its final state is usable.
    changedBooks.forEach(P2pOrderBook::validate);
    long n = diff.height.equals(cursor.height) ? Math.addExact(cursor.framesAtHeight, 1) : 1;
    cursor = new Cursor(diff.height, n);
    decoded.appliedNanos = System.nanoTime();
  }

  private void maybeLive(Stamp stamp) {
    if (workerToken != generation.get() || !seeded || !liveStatus || replayRemaining != 0 || snapshotPending) return;
    if (liveHeight != null && liveHeight.compareTo(cursor.height) > 0) cursor = new Cursor(liveHeight, 0);
    setReadiness(Readiness.LIVE);
    publishAll(stamp == null ? null : stamp, null);
  }

  private void publish(Decoded decoded) {
    publishAll(new Stamp(decoded.micros, decoded.nanos), decoded);
  }

  private void publishAll(Stamp stamp, Decoded decoded) {
    long token = workerToken;
    if (token != generation.get() || readiness != Readiness.LIVE) return;
    Set<String> changed = new LinkedHashSet<>();
    if (decoded == null) changed.addAll(coins);
    else decoded.diff.changes.forEach(change -> changed.add(change.coin));
    // A snapshot-only publication has no fabricated socket receive timestamp.
    long micros = stamp == null ? 0 : stamp.micros;
    long nanos = stamp == null ? 0 : stamp.nanos;
    for (String coin : changed) lastViews.put(coin, booksByCoin.get(coin).snapshot(micros, nanos));
    Map<String, P2pOrderBook.View> views = new LinkedHashMap<>(lastViews);
    long readyNanos = System.nanoTime();
    if (token != generation.get() || readiness != Readiness.LIVE) return;
    publications.onNext(new Publication(token, Collections.unmodifiableMap(views)));
    if (decoded != null) {
      for (String coin : changed) observations.onNext(new Observation(coin, token, decoded.diff,
          decoded.micros, decoded.nanos, decoded.decodeStartedNanos, decoded.decodedNanos,
          decoded.applyStartedNanos, decoded.appliedNanos, readyNanos));
    }
  }

  private void setRecovery(Recovery value) {
    synchronized (stateGate) {
      if (workerToken == generation.get() && !stopped.get()) recovery = value;
    }
  }

  private void setReadiness(Readiness value) {
    synchronized (stateGate) {
      if (workerToken != generation.get() || stopped.get()) return;
      readiness = value;
    }
    readinessEvents.onNext(value);
  }

  private void fail(Throwable error) {
    synchronized (stateGate) {
      if (workerToken != generation.get() || stopped.get()) return;
      lastFailure = error;
      generation.incrementAndGet();
      readiness = Readiness.FAILED;
    }
    CompletableSubject waiting = preparation;
    preparation = null;
    if (waiting != null) waiting.onError(error);
    setAutoReconnect(false);
    Channel channel = activeChannel;
    if (channel != null) channel.close();
    readinessEvents.onNext(Readiness.FAILED);
  }

  /** Immediate atomic invalidation; never wait for book work on a socket callback. */
  private void invalidate(long token, Throwable error, boolean cold) {
    synchronized (stateGate) {
      if (stopped.get() || readiness == Readiness.FAILED || !generation.compareAndSet(token, token + 1)) return;
      lastFailure = error;
      if (cold) recovery = Recovery.COLD;
      readiness = Readiness.CATCHING_UP;
    }
    CompletableSubject waiting = preparation;
    preparation = null;
    if (waiting != null) waiting.onError(error);
    Future<?> task = snapshotTask;
    if (task != null && !task.isDone()) task.cancel(true);
    Channel channel = activeChannel;
    if (channel != null) channel.close();
    // Notifications are queued; the immediately readable readiness/generation are already invalid.
    enqueue(generation.get(), 0, () -> readinessEvents.onNext(readiness), System.nanoTime());
  }

  private boolean enqueue(long token, long bytes, Work action, long ingressNanos) {
    if (stopped.get() || token != generation.get()) return false;
    if (queuedBytes.addAndGet(bytes) > MAX_PENDING_BYTES) {
      queuedBytes.addAndGet(-bytes);
      invalidate(token, new IOException("P2P ingress byte limit"), false);
      return false;
    }
    try {
      work.execute(new Task(token, bytes, action, ingressNanos));
      return true;
    }
    catch (RejectedExecutionException e) {
      queuedBytes.addAndGet(-bytes);
      // Do not recursively enqueue an invalidation event into a full queue.
      synchronized (stateGate) {
        if (generation.compareAndSet(token, token + 1)) {
          readiness = Readiness.CATCHING_UP;
          lastFailure = new IOException("P2P ingress queue overflow", e);
          CompletableSubject waiting = preparation;
          preparation = null;
          if (waiting != null) waiting.onError(lastFailure);
          Channel channel = activeChannel;
          if (channel != null) channel.close();
        }
      }
      return false;
    }
  }

  private final class Task implements Runnable {
    final long token;
    final long bytes;
    final Work action;
    final long ingressNanos;
    Task(long token, long bytes, Work action, long ingressNanos) {
      this.token = token; this.bytes = bytes; this.action = action; this.ingressNanos = ingressNanos;
    }
    @Override public void run() {
      try {
        if (token != generation.get() || stopped.get()) return;
        if (System.nanoTime() - ingressNanos > MAX_QUEUE_AGE_NANOS) {
          invalidate(token, new IOException("P2P work queue age limit"), false);
          return;
        }
        executingSinceNanos = System.nanoTime();
        workerToken = token;
        action.run();
      } catch (Throwable e) {
        // Even if overflow/close already invalidated this generation during apply, a
        // partially applied malformed frame must never become a retained warm seed.
        synchronized (stateGate) {
          if (!stopped.get() && readiness != Readiness.FAILED) recovery = Recovery.COLD;
        }
        if (workerToken == generation.get()) invalidate(workerToken, e, true);
      } finally {
        executingSinceNanos = 0;
        queuedBytes.addAndGet(-bytes);
      }
    }
  }

  private void checkHealth() {
    if (stopped.get() || readiness == Readiness.FAILED) return;
    long now = System.nanoTime();
    BinaryHandler handler = activeHandler;
    long fragmentStarted = handler == null ? 0 : handler.fragmentStartedNanos;
    if (fragmentStarted != 0 && now - fragmentStarted > MAX_QUEUE_AGE_NANOS) {
      invalidate(handler.token, new IOException("P2P fragment age limit"), true);
      return;
    }
    Runnable queued = work.getQueue().peek();
    boolean oldQueue = queued instanceof Task && now - ((Task) queued).ingressNanos > MAX_QUEUE_AGE_NANOS;
    boolean oldWork = executingSinceNanos != 0 && now - executingSinceNanos > MAX_QUEUE_AGE_NANOS;
    boolean silence = activeChannel != null && activeChannel.isOpen() && lastIngressNanos != 0
        && now - lastIngressNanos > MAX_INGRESS_SILENCE_NANOS;
    boolean negotiationTimeout = !negotiated && negotiationSinceNanos != 0
        && now - negotiationSinceNanos > TimeUnit.SECONDS.toNanos(10);
    if (oldQueue || oldWork || silence || negotiationTimeout) {
      invalidate(generation.get(), new IOException("P2P ingress/queue/negotiation timeout"), false);
    }
  }

  @Override public Completable disconnect() {
    synchronized (stateGate) {
      stopped.set(true);
      generation.incrementAndGet();
      readiness = Readiness.DISCONNECTED;
    }
    CompletableSubject waiting = preparation;
    preparation = null;
    if (waiting != null) waiting.onError(new IOException("P2P disconnected"));
    Future<?> task = snapshotTask;
    if (task != null) task.cancel(true);
    watchdog.shutdownNow();
    http.shutdownNow();
    work.shutdownNow();
    readinessEvents.onNext(Readiness.DISCONNECTED);
    return super.disconnect();
  }

  @Override protected WebSocketClientHandler getWebSocketClientHandler(
      WebSocketClientHandshaker handshaker, WebSocketClientHandler.WebSocketMessageHandler handler) {
    return new BinaryHandler(handshaker, handler, generation.get());
  }

  final class BinaryHandler extends NettyWebSocketClientHandler {
    private final long token;
    private ByteArrayOutputStream fragments;
    private boolean binary;
    private long fragmentMicros;
    private long fragmentNanos;
    private volatile long fragmentStartedNanos;
    BinaryHandler(WebSocketClientHandshaker handshaker, WebSocketMessageHandler handler, long token) {
      super(handshaker, handler);
      this.token = token;
    }
    @Override public void handlerAdded(ChannelHandlerContext ctx) {
      super.handlerAdded(ctx);
      activeChannel = ctx.channel();
      activeHandler = this;
      SslHandler ssl = ctx.pipeline().get(SslHandler.class);
      if (ssl != null) {
        SSLParameters parameters = ssl.engine().getSSLParameters();
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        ssl.engine().setSSLParameters(parameters);
      }
    }
    @Override public void channelRead0(ChannelHandlerContext ctx, Object message) throws Exception {
      if (token != generation.get() || stopped.get()) { ctx.close(); return; }
      long nanos = System.nanoTime();
      Instant now = Instant.now();
      long micros = now.getEpochSecond() * 1_000_000L + now.getNano() / 1000;
      lastIngressNanos = nanos; // Includes Block/Ping/Pong, never depends on a price change.
      if (!handshaker.isHandshakeComplete() || !(message instanceof WebSocketFrame)) {
        super.channelRead0(ctx, message);
        return;
      }
      if (!(message instanceof BinaryWebSocketFrame) && !(message instanceof TextWebSocketFrame)
          && !(message instanceof ContinuationWebSocketFrame)) {
        super.channelRead0(ctx, message);
        return;
      }
      WebSocketFrame frame = (WebSocketFrame) message;
      boolean continuation = frame instanceof ContinuationWebSocketFrame;
      if ((continuation && (fragments == null || nanos - fragmentNanos > MAX_QUEUE_AGE_NANOS))
          || (!continuation && fragments != null)) {
        invalidate(token, new IOException("Invalid fragmented message"), true);
        return;
      }
      if (!continuation) {
        binary = frame instanceof BinaryWebSocketFrame;
        fragmentMicros = micros;
        fragmentNanos = nanos;
      }
      int limit = binary ? codec.getMaxFrameBytes() : 65_536;
      int length = frame.content().readableBytes();
      if (length > limit - (fragments == null ? 0 : fragments.size())) {
        fragments = null;
        fragmentStartedNanos = 0;
        invalidate(token, new IOException("Fragment size limit"), true);
        return;
      }
      byte[] copy = new byte[length];
      frame.content().getBytes(frame.content().readerIndex(), copy);
      if (!frame.isFinalFragment() || continuation) {
        if (fragments == null) {
          fragments = new ByteArrayOutputStream();
          fragmentStartedNanos = fragmentNanos;
        }
        fragments.write(copy);
        if (!frame.isFinalFragment()) return;
        copy = fragments.toByteArray();
        fragments = null;
        fragmentStartedNanos = 0;
      }
      if (binary) binary(token, copy, fragmentMicros, fragmentNanos);
      else {
        final byte[] controlBytes = copy;
        enqueue(token, copy.length, () -> {
          String text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(controlBytes)).toString();
          control(text, token);
        }, fragmentNanos);
      }
    }
    @Override public void channelInactive(ChannelHandlerContext ctx) {
      fragments = null;
      fragmentStartedNanos = 0;
      invalidate(token, new IOException("P2P socket closed"), false);
      super.channelInactive(ctx); // Keep native reconnect ownership and stale-channel fences.
    }
    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable error) {
      fragments = null;
      fragmentStartedNanos = 0;
      invalidate(token, error, true);
      if (!handshakeFuture().isDone()) ((io.netty.channel.ChannelPromise) handshakeFuture()).tryFailure(error);
      ctx.close();
    }
  }

  interface SnapshotSource {
    P2pWireCodec.Snapshot fetch(Set<String> coins, BigInteger minHeight) throws Exception;
  }
  private interface Work { void run() throws Exception; }
  private static final class Stamp {
    final long micros; final long nanos;
    Stamp(long micros, long nanos) { this.micros = micros; this.nanos = nanos; }
  }
  private static final class Decoded {
    final P2pWireCodec.Diff diff;
    final long micros, nanos, decodeStartedNanos, decodedNanos;
    final int bytes;
    long applyStartedNanos, appliedNanos;
    Decoded(P2pWireCodec.Diff diff, long micros, long nanos, long decodeStartedNanos, long decodedNanos, int bytes) {
      this.diff = diff; this.micros = micros; this.nanos = nanos;
      this.decodeStartedNanos = decodeStartedNanos;
      this.decodedNanos = decodedNanos; this.bytes = bytes;
    }
  }
  private static final class Publication {
    final long generation;
    final Map<String, P2pOrderBook.View> books;
    Publication(long generation, Map<String, P2pOrderBook.View> books) {
      this.generation = generation; this.books = books;
    }
  }

  public static final class Cursor {
    private final BigInteger height;
    private final long framesAtHeight;
    Cursor(BigInteger height, long framesAtHeight) {
      this.height = P2pWireCodec.unsigned(height); this.framesAtHeight = framesAtHeight;
    }
    public BigInteger getHeight() { return height; }
    public long getFramesAtHeight() { return framesAtHeight; }
    @Override public String toString() { return "(" + height + "," + framesAtHeight + ")"; }
  }

  /** Publication diagnostics only. All nanoTime values share this JVM's monotonic clock. */
  public static final class Observation {
    private final String coin;
    private final long generation, ingressEpochMicros, ingressNanos, decodeStartedNanos, decodedNanos;
    private final long applyStartedNanos, appliedNanos, readyNanos;
    private final BigInteger height, sequence, blockTimeMicros;
    Observation(String coin, long generation, P2pWireCodec.Diff diff, long micros, long nanos,
        long decodeStartedNanos, long decodedNanos, long applyStartedNanos, long appliedNanos, long readyNanos) {
      this.coin = coin; this.generation = generation; this.height = diff.height;
      this.sequence = diff.sequence; this.blockTimeMicros = diff.blockTimeMicros;
      this.ingressEpochMicros = micros; this.ingressNanos = nanos;
      this.decodeStartedNanos = decodeStartedNanos; this.decodedNanos = decodedNanos;
      this.applyStartedNanos = applyStartedNanos; this.appliedNanos = appliedNanos; this.readyNanos = readyNanos;
    }
    public String getCoin() { return coin; }
    public long getGeneration() { return generation; }
    public BigInteger getHeight() { return height; }
    public BigInteger getSequence() { return sequence; }
    public BigInteger getBlockTimeMicros() { return blockTimeMicros; }
    public long getIngressEpochMicros() { return ingressEpochMicros; }
    public long getIngressNanos() { return ingressNanos; }
    public long getDecodeStartedNanos() { return decodeStartedNanos; }
    public long getDecodedNanos() { return decodedNanos; }
    public long getApplyStartedNanos() { return applyStartedNanos; }
    public long getAppliedNanos() { return appliedNanos; }
    public long getReadyNanos() { return readyNanos; }
  }
}
