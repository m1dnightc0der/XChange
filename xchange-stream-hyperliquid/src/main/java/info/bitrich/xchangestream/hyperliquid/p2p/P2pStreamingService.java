package info.bitrich.xchangestream.hyperliquid.p2p;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.service.netty.NettyStreamingService;
import info.bitrich.xchangestream.service.netty.WebSocketClientHandler;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.extensions.WebSocketClientExtensionHandler;
import io.netty.handler.flow.FlowControlHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.SSLParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provider-local binary handler and ordered state machine on the existing Netty transport hooks. */
public class P2pStreamingService extends NettyStreamingService<String> {
  public enum Readiness { DISCONNECTED, SEEDING, CATCHING_UP, LIVE, FAILED }
  private enum Recovery { COLD, WARM, LIVE_FIRST }
  private enum Subscription { BOOK, TRADES }
  private static final Logger LOG = LoggerFactory.getLogger(P2pStreamingService.class);
  private static final long MAX_QUEUE_AGE_NANOS = TimeUnit.SECONDS.toNanos(5);
  private static final long MAX_INGRESS_SILENCE_NANOS = TimeUnit.SECONDS.toNanos(30);
  private static final long MAX_PENDING_BYTES = 32L * 1024 * 1024;
  private static final int MAX_PENDING_FRAMES = 4096;
  private final Set<String> coins;
  private final Set<String> bookCoins;
  private final Set<String> tradeCoins;
  private final P2pWireCodec codec;
  private final SnapshotSource snapshotSource;
  private final ObjectMapper mapper = new ObjectMapper();
  private final ThreadPoolExecutor work;
  private final ExecutorService http = Executors.newSingleThreadExecutor(daemon("p2p-snapshot"));
  private final ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor(daemon("p2p-health"));
  private final Object stateGate = new Object();
  private final AtomicLong generation = new AtomicLong();
  // Includes raw flow-queued frames, fragment copies and the worker handoff, without double counting.
  private final AtomicLong queuedBytes = new AtomicLong();
  private final AtomicLong queuedFrames = new AtomicLong();
  private final AtomicBoolean stopped = new AtomicBoolean();
  private final Subject<Readiness> readinessEvents = BehaviorSubject.<Readiness>createDefault(Readiness.DISCONNECTED).toSerialized();
  private final Subject<Publication> publications = BehaviorSubject.<Publication>create().toSerialized();
  private final Subject<Observation> observations = PublishSubject.<Observation>create().toSerialized();
  private final Subject<TradePublication> tradePublications = PublishSubject.<TradePublication>create().toSerialized();
  private volatile Cursor tradeCursor;
  private volatile String lastTradeContinuityWarning;
  private volatile boolean bookLive;
  private volatile Readiness readiness = Readiness.DISCONNECTED;
  private volatile Cursor cursor;
  private volatile Throwable lastFailure;
  private volatile Recovery recovery = Recovery.COLD;
  private volatile Channel activeChannel;
  private volatile BinaryHandler activeHandler;
  private volatile Future<?> snapshotTask;
  private volatile long executingSinceNanos;
  private volatile long executingToken = -1;
  private volatile long negotiationSinceNanos;
  private volatile long negotiationToken = -1;
  private volatile boolean negotiated;
  private volatile CompletableSubject preparation;
  // All reconstruction fields below have exactly one writer: work.
  private long workerToken;
  private int totalOrders;
  private Stamp lastLiveStamp;
  private final Map<String, P2pOrderBook.View> lastViews = new LinkedHashMap<>();
  private final Map<String, BookProvenance> bookProvenance = new LinkedHashMap<>();
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
  private Subscription pendingSubscription;
  private BigInteger nextTradeSequence = BigInteger.ONE;
  private BigInteger lastTradeFrameHeight;
  private long tradeReplayRemaining;
  private boolean tradeSubscribed;
  private boolean tradeLive;
  private PartialTrade partialTrade;

  public P2pStreamingService(URI websocket, Set<String> coins, P2pSnapshotClient snapshots) {
    this(websocket, coins, Collections.emptySet(), snapshots);
  }

  public P2pStreamingService(URI websocket, Set<String> bookCoins, Set<String> tradeCoins,
      P2pSnapshotClient snapshots) {
    this(websocket, bookCoins, tradeCoins, new P2pWireCodec(),
        snapshots == null ? null : snapshots::fetch, 256);
  }

  P2pStreamingService(URI websocket, Set<String> coins, P2pWireCodec codec,
      SnapshotSource snapshotSource, int queueCapacity) {
    this(websocket, coins, Collections.emptySet(), codec, snapshotSource, queueCapacity);
  }

  P2pStreamingService(URI websocket, Set<String> bookCoins, Set<String> tradeCoins,
      P2pWireCodec codec, SnapshotSource snapshotSource, int queueCapacity) {
    super(validateWebSocket(websocket).toString(), codec.getMaxFrameBytes(),
        Duration.ofSeconds(10), Duration.ofSeconds(5), 5);
    if (bookCoins == null || tradeCoins == null || queueCapacity < 1) {
      throw new IllegalArgumentException("Invalid P2P membership or queue capacity");
    }
    Set<String> union = new LinkedHashSet<>(bookCoins);
    union.addAll(tradeCoins);
    if (union.isEmpty() || union.size() > 512) {
      throw new IllegalArgumentException("P2P membership must contain 1..512 coins");
    }
    for (String coin : union) {
      if (coin == null || coin.isEmpty() || coin.contains(",") || coin.length() > 255) {
        throw new IllegalArgumentException("Invalid coin");
      }
    }
    if (!bookCoins.isEmpty() && snapshotSource == null) {
      throw new IllegalArgumentException("Book membership requires a snapshot source");
    }
    this.coins = Collections.unmodifiableSet(union);
    this.bookCoins = Collections.unmodifiableSet(new LinkedHashSet<>(bookCoins));
    this.tradeCoins = Collections.unmodifiableSet(new LinkedHashSet<>(tradeCoins));
    this.codec = codec;
    this.snapshotSource = snapshotSource;
    this.work = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(queueCapacity), daemon("p2p-books"), new ThreadPoolExecutor.AbortPolicy());
    this.bookCoins.forEach(coin -> booksByCoin.put(coin, new P2pOrderBook(coin)));
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
  /** Initial DISCONNECTED and recoverable socket failures are not terminal. */
  public boolean isTerminal() { return stopped.get() || readiness == Readiness.FAILED; }
  public long getGeneration() { return generation.get(); }
  public Cursor getCursor() { return cursor; }
  public Set<String> getSubscribedCoins() { return coins; }
  public Set<String> getBookCoins() { return bookCoins; }
  public Set<String> getTradeCoins() { return tradeCoins; }
  public Cursor getTradeCursor() { return tradeCursor; }
  public String getLastTradeContinuityWarning() { return lastTradeContinuityWarning; }
  public Throwable getLastFailure() { return lastFailure; }
  public Observable<Readiness> readiness() { return readinessEvents.filter(value -> value == readiness); }
  public Observable<Observation> observations() {
    return observations.filter(value -> value.generation == generation.get() && bookLive);
  }

  public Observable<P2pOrderBook.View> book(String coin) {
    if (!bookCoins.contains(coin)) return Observable.error(new IllegalArgumentException("Unsubscribed book coin"));
    return publications.filter(p -> bookLive && p.generation == generation.get())
        .map(p -> p.books.get(coin)).distinctUntilChanged();
  }

  public Observable<TradePublication> trades(String coin) {
    if (!tradeCoins.contains(coin)) return Observable.error(new IllegalArgumentException("Unsubscribed trade coin"));
    // Generation is checked once at dispatch, not per observer: callbacks may revoke it.
    return tradePublications.filter(p -> coin.equals(p.event.coin));
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
      return prepared.andThen(Completable.defer(() -> {
        if (isTerminal()) return Completable.error(new IOException("P2P connection cancelled"));
        long current = generation.get();
        return super.connect().doOnComplete(() -> {
          synchronized (stateGate) {
            if (current != generation.get() || isTerminal()) return;
            negotiationSinceNanos = System.nanoTime();
            negotiationToken = current;
            BinaryHandler handler = activeHandler;
            if (handler != null && handler.token == current) handler.lastIngressNanos = negotiationSinceNanos;
          }
          sendControl("{\"method\":\"info\"}", current);
        });
      }));
    });
  }

  private void prepare(CompletableSubject prepared) {
    long token;
    synchronized (stateGate) { token = generation.incrementAndGet(); }
    workerToken = token;
    preparation = prepared;
    negotiated = false;
    subscribed = false;
    bookLive = false;
    pendingSubscription = null;
    tradeSubscribed = false;
    tradeLive = false;
    nextTradeSequence = BigInteger.ONE;
    lastTradeFrameHeight = null;
    tradeReplayRemaining = tradeCursor == null ? 0 : tradeCursor.framesAtHeight;
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
    if (bookCoins.isEmpty()) {
      setReadiness(Readiness.CATCHING_UP);
      preparation = null;
      prepared.onComplete();
      return;
    }
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
        P2pWireCodec.Snapshot snapshot = snapshotSource.fetch(bookCoins, minHeight);
        enqueue(token, 0, () -> {
          try {
          if (!snapshot.books.keySet().equals(bookCoins) || snapshot.height.compareTo(minHeight) < 0) {
            throw new IllegalArgumentException("Snapshot membership/height mismatch");
          }
          Map<String, P2pOrderBook> replacement = new LinkedHashMap<>();
          snapshot.books.forEach((coin, orders) -> {
            P2pOrderBook book = new P2pOrderBook(coin);
            book.seed(orders);
            replacement.put(coin, book);
          });
          if (snapshot.height.equals(P2pWireCodec.MAX_U64)) throw new IllegalArgumentException("Height exhausted");
          int replacementOrders = replacement.values().stream().mapToInt(P2pOrderBook::getOrderCount).sum();
          synchronized (stateGate) {
            if (token != generation.get() || stopped.get()) return;
            booksByCoin = replacement;
            totalOrders = replacementOrders;
            lastViews.clear();
            bookProvenance.clear(); // Snapshot metadata has no exchange block time.
            cursor = new Cursor(snapshot.height.add(BigInteger.ONE), 0);
            if (!tradeCoins.isEmpty() && tradeCursor == null) tradeCursor = new Cursor(cursor.height, 0);
            seeded = true;
            snapshotPending = false;
            replayRemaining = 0;
          }
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
    if (token != generation.get() || stopped.get()) return;
    if (root == null || !root.isObject()) { fail(new IOException("Invalid control message")); return; }
    String channel = root.path("channel").asText();
    String error = root.path("error").asText();
    if (error.isEmpty()) error = root.path("error").path("code").asText();
    if (error.isEmpty()) error = root.path("code").asText();
    if (error.isEmpty() && "error".equals(channel)) {
      JsonNode data = root.path("data");
      error = data.isTextual() ? data.asText() : data.path("code").asText();
    }
    if (!error.isEmpty() || "error".equals(channel) || "errors".equals(channel)) {
      Subscription owner = errorOwner(root);
      if (owner == null || (owner == Subscription.BOOK ? !subscribed : !tradeSubscribed)) {
        fail(new IOException("Unattributable P2P control error"));
        return;
      }
      if ("history_busy".equals(error)) {
        invalidate(token, new IOException("P2P history busy"), false);
      } else if ("history_unavailable".equals(error)) {
        if (owner == Subscription.BOOK) {
          synchronized (stateGate) {
            if (token != generation.get() || stopped.get()) return;
            seeded = false;
            bookLive = false;
            recovery = Recovery.LIVE_FIRST;
          }
          invalidate(token, new IOException("Book history unavailable"), false);
        } else {
          JsonNode oldest = controlField(root, "oldest_height");
          BigInteger boundary;
          try {
            if (!(oldest.isIntegralNumber() || oldest.isTextual())
                || !oldest.asText().matches("[0-9]{1,20}")) {
              throw new IllegalArgumentException("Missing oldest height");
            }
            boundary = P2pWireCodec.unsigned(new BigInteger(oldest.asText())).add(BigInteger.valueOf(100));
            P2pWireCodec.unsigned(boundary);
            if (tradeCursor == null || boundary.compareTo(tradeCursor.height) <= 0) {
              throw new IllegalArgumentException("No forward trade recovery boundary");
            }
          } catch (RuntimeException invalid) {
            fail(new IOException("Trade continuity failure: no safe retention boundary", invalid));
            return;
          }
          BigInteger from;
          synchronized (stateGate) {
            if (token != generation.get() || stopped.get()) return;
            from = tradeCursor.height;
            tradeCursor = new Cursor(boundary, 0);
            partialTrade = null;
            tradeReplayRemaining = 0;
            tradeLive = false;
          }
          warnTradeGap(from, boundary.subtract(BigInteger.ONE), "retention");
          invalidate(token, new IOException("Trade history unavailable"), false);
        }
      } else {
        fail(new IOException("P2P control error"));
      }
      return;
    }
    if (!"info".equals(channel)) return; // esp/prime/subscribe have no success acknowledgement.
    if (negotiated || !root.path("binary_version").isIntegralNumber()
        || root.path("binary_version").asInt() != 3
        || (!bookCoins.isEmpty() && !"on".equals(root.path("streams").path("book").asText()))
        || (!tradeCoins.isEmpty() && !"on".equals(root.path("streams").path("trades").asText()))) {
      fail(new IOException("P2P requires v3 and enabled requested streams"));
      return;
    }
    negotiated = true;
    sendControl("{\"method\":\"esp\",\"version\":3}", token);
    subscribeNext();
  }

  private JsonNode controlField(JsonNode root, String name) {
    if (root.has(name)) return root.path(name);
    if (root.path("error").has(name)) return root.path("error").path(name);
    if (root.path("data").has(name)) return root.path("data").path(name);
    return root.path("data").path("error").path(name);
  }

  private Subscription errorOwner(JsonNode root) {
    JsonNode stream = controlField(root, "stream");
    if (!stream.isMissingNode()) {
      if ("book".equals(stream.asText()) || "0".equals(stream.asText())) return Subscription.BOOK;
      if ("trades".equals(stream.asText()) || "3".equals(stream.asText())) return Subscription.TRADES;
      return null;
    }
    String channel = root.path("channel").asText();
    if ("book".equals(channel)) return Subscription.BOOK;
    if ("trades".equals(channel)) return Subscription.TRADES;
    return pendingSubscription;
  }

  private void subscribeNext() {
    long token = workerToken;
    if (!negotiated || pendingSubscription != null || token != generation.get() || stopped.get()) return;
    Map<String, Object> subscribe = new LinkedHashMap<>();
    subscribe.put("method", "subscribe");
    if (!bookCoins.isEmpty() && !subscribed) {
      pendingSubscription = Subscription.BOOK;
      subscribe.put("coins", bookCoins);
      if (recovery != Recovery.LIVE_FIRST) subscribe.put("from_height", cursor.height);
      subscribed = true;
    } else if (!tradeCoins.isEmpty() && !tradeSubscribed && tradeCursor != null) {
      pendingSubscription = Subscription.TRADES;
      subscribe.put("stream", "trades");
      subscribe.put("coins", tradeCoins);
      subscribe.put("from_height", tradeCursor.height);
      tradeSubscribed = true;
    } else {
      return;
    }
    try { sendControl(mapper.writeValueAsString(subscribe), token); }
    catch (IOException error) { fail(error); }
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
    long token = workerToken;
    long decodedNanos = System.nanoTime();
    // Decoding may overlap socket closure, watchdog invalidation or explicit stop.
    if (token != generation.get() || stopped.get()) return;
    if (frame instanceof P2pWireCodec.Ignored) return;
    if (frame instanceof P2pWireCodec.Block) {
      synchronized (stateGate) {
        if (token != generation.get() || stopped.get()) return;
        if (negotiated && bookCoins.isEmpty() && !tradeCoins.isEmpty() && tradeCursor == null) {
          tradeCursor = new Cursor(((P2pWireCodec.Block) frame).height, 0);
        }
      }
      subscribeNext();
      return;
    }
    if (frame instanceof P2pWireCodec.TradeBatch) {
      if (!tradeCoins.isEmpty()) processTrades((P2pWireCodec.TradeBatch) frame);
      return;
    }
    if (frame instanceof P2pWireCodec.Status && ((P2pWireCodec.Status) frame).stream != 0) {
      P2pWireCodec.Status status = (P2pWireCodec.Status) frame;
      if (status.stream == 3 && !tradeCoins.isEmpty()) tradeStatus(status);
      return;
    }
    if (!negotiated || !subscribed) {
      fail(new IllegalArgumentException("Book traffic before negotiation"));
      return;
    }
    if (frame instanceof P2pWireCodec.Status) {
      P2pWireCodec.Status status = (P2pWireCodec.Status) frame;
      if (status.stream != 0) return;
      if (status.state == 2) {
        invalidate(token, new IOException("Provider book gap"), true);
      } else if (status.state == 0) {
        liveStatus = false;
        bookLive = false;
        updateReadiness();
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
      invalidate(token, new IOException("Book sequence discontinuity"), false);
      return;
    }
    nextSequence = nextSequence.add(BigInteger.ONE);
    if (lastFrameHeight != null && diff.height.compareTo(lastFrameHeight) < 0) {
      throw new IllegalArgumentException("Regressing book height");
    }
    lastFrameHeight = diff.height;
    for (P2pWireCodec.Change change : diff.changes) {
      if (!bookCoins.contains(change.coin)) throw new IllegalArgumentException("Unexpected book coin");
    }
    Decoded decoded = new Decoded(diff, micros, nanos, decodeStartedNanos, decodedNanos, bytes);
    if (!seeded) {
      if (recovery != Recovery.LIVE_FIRST) throw new IllegalArgumentException("Unseeded diff");
      if (pending.size() >= MAX_PENDING_FRAMES || pendingBytes + bytes > MAX_PENDING_BYTES) {
        throw new IllegalArgumentException("Live-first buffer overflow");
      }
      pending.add(decoded);
      pendingBytes += bytes;
      if (!snapshotPending) requestSnapshot(token, diff.height, null);
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
    if (bookLive) publish(decoded);
  }

  private void updateReadiness() {
    if ((bookCoins.isEmpty() || bookLive) && (tradeCoins.isEmpty() || tradeLive)) {
      setReadiness(Readiness.LIVE);
    } else {
      setReadiness(!bookCoins.isEmpty() && !seeded ? Readiness.SEEDING : Readiness.CATCHING_UP);
    }
  }

  private void tradeStatus(P2pWireCodec.Status status) {
    IOException error = null;
    synchronized (stateGate) {
      if (workerToken != generation.get() || stopped.get()) return;
      if (!negotiated || !tradeSubscribed || tradeCursor == null) {
        error = new IOException("Trade status before subscription");
      } else if (status.state == 2) {
        if (status.toHeight.compareTo(tradeCursor.height) < 0
            || status.toHeight.equals(P2pWireCodec.MAX_U64)) {
          error = new IOException("Trade continuity failure: invalid gap boundary");
        } else {
          // No book snapshot repairs the explicitly unrecoverable inclusive range.
          tradeCursor = new Cursor(status.toHeight.add(BigInteger.ONE), 0);
          tradeReplayRemaining = 0;
          partialTrade = null;
          tradeLive = false;
        }
      } else if (status.state == 0) {
        tradeLive = false;
      } else if (tradeReplayRemaining != 0 || partialTrade != null) {
        error = new IOException("Trade continuity failure: incomplete replay");
      } else {
        if (status.height.compareTo(tradeCursor.height) > 0) tradeCursor = new Cursor(status.height, 0);
        tradeLive = true;
        if (pendingSubscription == Subscription.TRADES) pendingSubscription = null;
      }
    }
    // Never invoke observers, logging appenders or transport callbacks under stateGate.
    if (error != null) { fail(error); return; }
    if (status.state == 2) warnTradeGap(status.height, status.toHeight, "provider gap");
    updateReadiness();
    subscribeNext();
  }

  private void warnTradeGap(BigInteger from, BigInteger to, String reason) {
    lastTradeContinuityWarning = "P2P trades missing heights [" + from + "," + to + "] (" + reason + ")";
    LOG.warn(lastTradeContinuityWarning);
  }

  private void processTrades(P2pWireCodec.TradeBatch batch) {
    long token = workerToken;
    if (!negotiated || !tradeSubscribed || tradeCursor == null) {
      fail(new IOException("Trade traffic before subscription"));
      return;
    }
    try {
      // Validate the complete batch, including replayed frames, before any publication.
      P2pWireCodec.unsigned(batch.height);
      P2pWireCodec.unsigned(batch.sequence);
      if (lastTradeFrameHeight != null && batch.height.compareTo(lastTradeFrameHeight) < 0) {
        throw new IllegalArgumentException("Regressing trade height");
      }
      if (batch.height.compareTo(tradeCursor.height) < 0) {
        throw new IllegalArgumentException("Trade frame before cursor");
      }
      for (P2pWireCodec.TradeEvent event : batch.trades) {
        if (event == null || !tradeCoins.contains(event.coin) || event.price == null
            || event.size == null || event.price.signum() <= 0 || event.size.signum() <= 0
            || event.buyer == null || event.seller == null) {
          throw new IllegalArgumentException("Invalid trade batch membership/value");
        }
        P2pWireCodec.unsigned(event.eventTimeMicros).divide(BigInteger.valueOf(1000)).longValueExact();
      }
      if (!batch.sequence.equals(nextTradeSequence)) {
        invalidate(token, new IOException("Trade sequence discontinuity"), false);
        return;
      }
      nextTradeSequence = nextTradeSequence.add(BigInteger.ONE);
      lastTradeFrameHeight = batch.height;
      if (tradeReplayRemaining > 0) {
        if (!batch.height.equals(tradeCursor.height)) {
          throw new IllegalArgumentException("Trade replay skipped cursor height");
        }
        tradeReplayRemaining--;
        return;
      }
      long ordinal = batch.height.equals(tradeCursor.height)
          ? Math.addExact(tradeCursor.framesAtHeight, 1) : 1;
      byte[] fingerprint = tradeFingerprint(batch);
      PartialTrade partial = partialTrade;
      if (partial == null) {
        partial = new PartialTrade(batch.height, ordinal, fingerprint);
        partialTrade = partial;
      } else if (!partial.height.equals(batch.height) || partial.ordinal != ordinal
          || !MessageDigest.isEqual(partial.fingerprint, fingerprint)
          || partial.emittedPrefix > batch.trades.size()) {
        throw new IllegalArgumentException("Trade replay differs from interrupted frame");
      }
      for (int i = partial.emittedPrefix; i < batch.trades.size(); i++) {
        if (token != generation.get() || stopped.get()) return;
        TradePublication publication = new TradePublication(
            "p2p:trades:" + batch.height + ":" + ordinal + ":" + i, batch.trades.get(i));
        // All current observers get this event even if the first one revokes the generation.
        if (token != generation.get() || stopped.get()) return;
        tradePublications.onNext(publication);
        partial.emittedPrefix = i + 1;
      }
      synchronized (stateGate) {
        if (token != generation.get() || stopped.get()) return;
        tradeCursor = new Cursor(batch.height, ordinal);
        partialTrade = null;
      }
    } catch (RuntimeException invalid) {
      fail(new IOException("Trade continuity failure: invalid batch or replay", invalid));
    }
  }

  private byte[] tradeFingerprint(P2pWireCodec.TradeBatch batch) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      fingerprintValue(digest, batch.height);
      fingerprintValue(digest, batch.trades.size());
      // Connection sequence and local/sent times are deliberately not part of replay identity.
      for (P2pWireCodec.TradeEvent event : batch.trades) {
        fingerprintValue(digest, event.eventTimeMicros);
        fingerprintValue(digest, event.buy);
        fingerprintValue(digest, event.hash);
        fingerprintValue(digest, event.coin);
        fingerprintValue(digest, event.price);
        fingerprintValue(digest, event.size);
        fingerprintValue(digest, event.direction);
        fingerprintSide(digest, event.buyer);
        fingerprintSide(digest, event.seller);
      }
      return digest.digest();
    } catch (NoSuchAlgorithmException unavailable) {
      throw new IllegalStateException("SHA-256 unavailable", unavailable);
    }
  }

  private void fingerprintSide(MessageDigest digest, P2pWireCodec.TradeSide side) {
    fingerprintValue(digest, side.user);
    fingerprintValue(digest, side.oid);
    fingerprintValue(digest, side.twapId);
    fingerprintValue(digest, side.cloid);
    fingerprintValue(digest, side.startPosition);
  }

  private void fingerprintValue(MessageDigest digest, Object value) {
    byte[] bytes = value == null ? null : value.toString().getBytes(StandardCharsets.UTF_8);
    digest.update(ByteBuffer.allocate(4).putInt(bytes == null ? -1 : bytes.length).array());
    if (bytes != null) digest.update(bytes);
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
    // Commit provenance only after the entire frame validates. Replay/status never enter here.
    BookProvenance provenance = new BookProvenance(diff.blockTimeMicros, cursor);
    for (P2pWireCodec.Change change : diff.changes) bookProvenance.put(change.coin, provenance);
    decoded.appliedNanos = System.nanoTime();
  }

  private void maybeLive(Stamp stamp) {
    if (workerToken != generation.get() || !seeded || !liveStatus || replayRemaining != 0 || snapshotPending) return;
    if (liveHeight != null && liveHeight.compareTo(cursor.height) > 0) cursor = new Cursor(liveHeight, 0);
    bookLive = true;
    if (pendingSubscription == Subscription.BOOK) pendingSubscription = null;
    updateReadiness();
    publishAll(stamp == null ? null : stamp, null);
    subscribeNext();
  }

  private void publish(Decoded decoded) {
    publishAll(new Stamp(decoded.micros, decoded.nanos), decoded);
  }

  private void publishAll(Stamp stamp, Decoded decoded) {
    long token = workerToken;
    if (token != generation.get() || !bookLive) return;
    Set<String> changed = new LinkedHashSet<>();
    if (decoded == null) changed.addAll(bookCoins);
    else decoded.diff.changes.forEach(change -> changed.add(change.coin));
    long nanos = stamp == null ? 0 : stamp.nanos;
    for (String coin : changed) {
      // Global status/other-coin progress must not replace this coin's applied diff identity.
      BookProvenance provenance = bookProvenance.get(coin);
      lastViews.put(coin, booksByCoin.get(coin).snapshot(
          provenance == null ? null : provenance.createdAtMicros, nanos,
          provenance == null ? null : provenance.cursor.height,
          provenance == null ? 0 : provenance.cursor.framesAtHeight));
    }
    Map<String, P2pOrderBook.View> views = new LinkedHashMap<>(lastViews);
    long readyNanos = System.nanoTime();
    if (token != generation.get() || !bookLive) return;
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

  /** One metadata-only diagnostic per successful failure transition, never on a provider executor. */
  private void recordFailure(long token, Readiness atFailure, Channel channel, BinaryHandler handler,
      Throwable error, String transition) {
    try {
      long now = System.nanoTime();
      long oldestAge = 0;
      IngressCredit ingress = handler == null || handler.token != token ? null : handler.credits.peek();
      if (ingress != null && !ingress.released.get()) oldestAge = Math.max(oldestAge, now - ingress.nanos);
      Runnable queued = work.getQueue().peek();
      if (queued instanceof Task && ((Task) queued).token == token) {
        oldestAge = Math.max(oldestAge, now - ((Task) queued).ingressNanos);
      }
      long runningToken = executingToken;
      long executingSince = executingSinceNanos;
      if (runningToken == token && executingSince != 0) {
        oldestAge = Math.max(oldestAge, now - executingSince);
      }
      long oldestAgeMillis = TimeUnit.NANOSECONDS.toMillis(oldestAge);
      int queueCount = work.getQueue().size();
      long bytes = queuedBytes.get();
      long frames = queuedFrames.get();
      String channelId = channel == null ? "none" : channel.id().asShortText();
      boolean open = channel != null && channel.isOpen();
      boolean active = channel != null && channel.isActive();
      boolean writable = channel != null && channel.isWritable();
      String reason = failureReason(error, transition);
      String causeClass = error == null ? "none" : error.getClass().getName();
      // Capture only scalars: no Throwable, payload, channel or service is retained by logging.
      // A slow logger must not occupy Netty, work, snapshots or the health watchdog for a successor.
      ForkJoinPool.commonPool().execute(() -> LOG.warn(
          "P2P failure generation={} readiness={} channel={} open={} active={} writable={} "
              + "queue_count={} queued_bytes={} queued_frames={} oldest_age_ms={} transition={} reason={} cause_class={}",
          token, atFailure, channelId, open, active, writable, queueCount, bytes, frames,
          oldestAgeMillis, transition, reason, causeClass));
    } catch (RuntimeException ignored) {
      // Diagnostics are best effort; rejection must never prevent fencing/closure or run a logger inline.
    }
  }

  private static String failureReason(Throwable error, String fallback) {
    // Inspect only the known JDK exception type. Never print a parser/observer/provider message.
    if (error != null && error.getClass() == IOException.class) {
      String message = error.getMessage();
      if ("P2P ingress byte limit".equals(message)) return "ingress_byte_limit";
      if ("P2P ingress frame limit".equals(message)) return "ingress_frame_limit";
      if ("P2P ingress queue overflow".equals(message)) return "worker_queue_overflow";
      if ("P2P ingress queue age limit".equals(message)) return "ingress_age_limit";
      if ("P2P work queue age limit".equals(message)) return "work_age_limit";
      if ("P2P fragment age limit".equals(message)) return "fragment_age_limit";
      if ("P2P ingress/queue/negotiation timeout".equals(message)) return "health_timeout";
      if ("P2P socket closed".equals(message)) return "socket_closed";
      if ("Invalid fragmented message".equals(message)) return "invalid_fragment";
      if ("Fragment size limit".equals(message)) return "fragment_size_limit";
      if ("P2P read resume rejected".equals(message)) return "read_resume_rejected";
      if ("P2P handoff completion rejected".equals(message)) return "handoff_completion_rejected";
      if ("P2P ingress handler removed".equals(message)) return "ingress_handler_removed";
      if ("P2P ingress admission bypassed".equals(message)) return "admission_bypassed";
    }
    return error instanceof IllegalArgumentException ? "invalid_data" : fallback;
  }

  private void fail(Throwable error) {
    Channel channel;
    BinaryHandler handler;
    Readiness atFailure;
    long token;
    CompletableSubject waiting;
    synchronized (stateGate) {
      if (workerToken != generation.get() || stopped.get()) return;
      token = workerToken;
      atFailure = readiness;
      handler = activeHandler;
      lastFailure = error;
      generation.incrementAndGet();
      readiness = Readiness.FAILED;
      channel = activeChannel;
      waiting = preparation;
      preparation = null;
    }
    recordFailure(token, atFailure, channel, handler, error, "terminal_failure");
    if (waiting != null) waiting.onError(error);
    setAutoReconnect(false);
    if (channel != null) channel.close();
    readinessEvents.onNext(Readiness.FAILED);
  }

  /** Immediate atomic invalidation; never wait for book work on a socket callback. */
  private void invalidate(long token, Throwable error, boolean cold) {
    Channel channel;
    BinaryHandler handler;
    Readiness atFailure;
    CompletableSubject waiting;
    Future<?> task;
    synchronized (stateGate) {
      if (stopped.get() || readiness == Readiness.FAILED || !generation.compareAndSet(token, token + 1)) return;
      atFailure = readiness;
      handler = activeHandler;
      lastFailure = error;
      if (cold) recovery = Recovery.COLD;
      readiness = Readiness.CATCHING_UP;
      channel = activeChannel;
      waiting = preparation;
      preparation = null;
      task = snapshotTask;
    }
    recordFailure(token, atFailure, channel, handler, error, cold ? "cold_recovery" : "warm_recovery");
    if (waiting != null) waiting.onError(error);
    if (task != null && !task.isDone()) task.cancel(true);
    if (channel != null) channel.close();
    // Notifications are queued; the immediately readable readiness/generation are already invalid.
    enqueue(token + 1, 0, () -> readinessEvents.onNext(readiness), System.nanoTime());
  }

  private boolean enqueue(long token, long bytes, Work action, long ingressNanos) {
    return enqueue(token, bytes, action, ingressNanos, null);
  }

  private boolean enqueue(long token, long bytes, Work action, long ingressNanos, Runnable completion) {
    if (isTerminal() || token != generation.get()) {
      if (completion != null) completion.run();
      return false;
    }
    if (queuedBytes.addAndGet(bytes) > MAX_PENDING_BYTES) {
      queuedBytes.addAndGet(-bytes);
      if (completion != null) completion.run();
      invalidate(token, new IOException("P2P ingress byte limit"), false);
      return false;
    }
    Task task = new Task(token, bytes, action, ingressNanos, completion);
    try {
      work.execute(task);
      return true;
    }
    catch (RejectedExecutionException e) {
      task.discard();
      // Do not recursively enqueue an invalidation event into a full queue.
      CompletableSubject waiting = null;
      Channel channel = null;
      BinaryHandler handler = null;
      Readiness atFailure = null;
      Throwable failure = new IOException("P2P ingress queue overflow", e);
      synchronized (stateGate) {
        if (!isTerminal() && generation.compareAndSet(token, token + 1)) {
          atFailure = readiness;
          handler = activeHandler;
          readiness = Readiness.CATCHING_UP;
          lastFailure = failure;
          waiting = preparation;
          preparation = null;
          channel = activeChannel;
        }
      }
      // Only the winning transition records a diagnostic; stale rejection is silent.
      if (atFailure != null) recordFailure(token, atFailure, channel, handler, failure, "worker_rejection");
      // Observer and transport callbacks must never run under stateGate.
      if (waiting != null) waiting.onError(failure);
      if (channel != null) channel.close();
      return false;
    }
  }

  private final class Task implements Runnable {
    final long token;
    final long bytes;
    final Work action;
    final long ingressNanos;
    final Runnable completion;
    final AtomicBoolean completed = new AtomicBoolean();
    Task(long token, long bytes, Work action, long ingressNanos, Runnable completion) {
      this.token = token; this.bytes = bytes; this.action = action; this.ingressNanos = ingressNanos;
      this.completion = completion;
    }
    void discard() {
      if (!completed.compareAndSet(false, true)) return;
      queuedBytes.addAndGet(-bytes);
      if (completion != null) completion.run();
    }
    @Override public void run() {
      try {
        if (completed.get() || token != generation.get() || isTerminal()) return;
        if (System.nanoTime() - ingressNanos > MAX_QUEUE_AGE_NANOS) {
          invalidate(token, new IOException("P2P work queue age limit"), false);
          return;
        }
        executingSinceNanos = System.nanoTime();
        executingToken = token;
        workerToken = token;
        action.run();
      } catch (Throwable e) {
        // Even if overflow/close already invalidated this generation during apply, a
        // partially applied malformed frame must never become a retained warm seed.
        synchronized (stateGate) {
          if (!stopped.get() && readiness != Readiness.FAILED) recovery = Recovery.COLD;
        }
        if (token == generation.get()) invalidate(token, e, true);
      } finally {
        executingSinceNanos = 0;
        executingToken = -1;
        discard();
      }
    }
  }

  private void checkHealth() {
    long token = generation.get();
    BinaryHandler handler = activeHandler;
    if (isTerminal()) return;
    if (handler != null && handler.token != token) handler = null;
    long now = System.nanoTime();
    long fragmentStarted = handler == null ? 0 : handler.fragmentStartedNanos;
    if (fragmentStarted != 0 && now - fragmentStarted > MAX_QUEUE_AGE_NANOS) {
      invalidate(token, new IOException("P2P fragment age limit"), true);
      return;
    }
    IngressCredit oldest = handler == null ? null : handler.credits.peek();
    boolean oldIngress = oldest != null && !oldest.released.get()
        && now - oldest.nanos > MAX_QUEUE_AGE_NANOS;
    Runnable queued = work.getQueue().peek();
    boolean oldQueue = queued instanceof Task && ((Task) queued).token == token
        && now - ((Task) queued).ingressNanos > MAX_QUEUE_AGE_NANOS;
    long runningToken = executingToken;
    long executingSince = executingSinceNanos;
    boolean oldWork = runningToken == token && executingSince != 0
        && now - executingSince > MAX_QUEUE_AGE_NANOS;
    long lastArrival = handler == null ? 0 : handler.lastIngressNanos;
    boolean silence = handler != null && handler.context.channel().isOpen() && lastArrival != 0
        && now - lastArrival > MAX_INGRESS_SILENCE_NANOS;
    long negotiationOwner = negotiationToken;
    long negotiationStarted = negotiationSinceNanos;
    boolean negotiationTimeout = negotiationOwner == token && !negotiated && negotiationStarted != 0
        && now - negotiationStarted > TimeUnit.SECONDS.toNanos(10);
    if (oldIngress || oldQueue || oldWork || silence || negotiationTimeout) {
      invalidate(token, new IOException("P2P ingress/queue/negotiation timeout"), false);
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
    setAutoReconnect(false);
    watchdog.shutdownNow();
    http.shutdownNow();
    for (Runnable queued : work.shutdownNow()) {
      if (queued instanceof Task) ((Task) queued).discard();
    }
    Channel channel = activeChannel;
    if (channel != null) channel.close();
    readinessEvents.onNext(Readiness.DISCONNECTED);
    return super.disconnect();
  }

  @Override protected WebSocketClientHandler getWebSocketClientHandler(
      WebSocketClientHandshaker handshaker, WebSocketClientHandler.WebSocketMessageHandler handler) {
    return new BinaryHandler(handshaker, handler, generation.get());
  }

  /** One reservation follows a frame from the flow queue through copying and worker completion. */
  private final class IngressCredit {
    final BinaryHandler owner;
    final long bytes, micros, nanos;
    final AtomicBoolean released = new AtomicBoolean();
    IngressCredit(BinaryHandler owner, long bytes, long micros, long nanos) {
      this.owner = owner; this.bytes = bytes; this.micros = micros; this.nanos = nanos;
    }
    void release() {
      if (!released.compareAndSet(false, true)) return;
      queuedBytes.addAndGet(-bytes);
      queuedFrames.decrementAndGet();
      owner.credits.remove(this);
    }
  }

  /** FlowControlHandler and SimpleChannelInboundHandler can both release this owned message. */
  private final class IngressMessage extends AbstractReferenceCounted {
    final Object message;
    final IngressCredit credit;
    private boolean transferred;
    IngressMessage(Object message, IngressCredit credit) {
      this.message = message; this.credit = credit;
    }
    IngressCredit transfer() {
      transferred = true;
      return credit;
    }
    @Override protected void deallocate() {
      try { ReferenceCountUtil.release(message); }
      finally { if (!transferred) credit.release(); }
    }
    @Override public ReferenceCounted touch(Object hint) {
      ReferenceCountUtil.touch(message, hint);
      return this;
    }
  }

  private final class IngressHandler extends ChannelInboundHandlerAdapter {
    private final BinaryHandler owner;
    IngressHandler(BinaryHandler owner) { this.owner = owner; }
    @Override public void channelRead(ChannelHandlerContext ctx, Object message) {
      if (!owner.current()) {
        ReferenceCountUtil.release(message);
        ctx.close();
        return;
      }
      long nanos = System.nanoTime();
      Instant now = Instant.now();
      long micros = now.getEpochSecond() * 1_000_000L + now.getNano() / 1000;
      long bytes = message instanceof ByteBufHolder ? ((ByteBufHolder) message).content().readableBytes() : 0;
      owner.lastIngressNanos = nanos; // Actual ingress, owned by this channel, before any flow-queue wait.
      long retainedBytes = queuedBytes.addAndGet(bytes);
      long retainedFrames = queuedFrames.incrementAndGet();
      if (retainedBytes > MAX_PENDING_BYTES || retainedFrames > MAX_PENDING_FRAMES) {
        queuedBytes.addAndGet(-bytes);
        queuedFrames.decrementAndGet();
        ReferenceCountUtil.release(message);
        invalidate(owner.token, new IOException(retainedBytes > MAX_PENDING_BYTES
            ? "P2P ingress byte limit" : "P2P ingress frame limit"), false);
        owner.closeGeneration();
        return;
      }
      IngressCredit credit = new IngressCredit(owner, bytes, micros, nanos);
      owner.credits.add(credit);
      ctx.fireChannelRead(new IngressMessage(message, credit));
    }
    @Override public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
      owner.closeGeneration();
      super.handlerRemoved(ctx);
    }
  }

  final class BinaryHandler extends NettyWebSocketClientHandler {
    private final long token;
    private final ConcurrentLinkedQueue<IngressCredit> credits = new ConcurrentLinkedQueue<>();
    private final List<IngressCredit> fragmentCredits = new ArrayList<>();
    private ChannelHandlerContext context;
    private volatile long lastIngressNanos;
    private volatile boolean closed;
    private volatile boolean handoff;
    private boolean demandScheduled;
    private boolean readPending;
    private ByteArrayOutputStream fragments;
    private boolean binary;
    private long fragmentMicros;
    private long fragmentNanos;
    private volatile long fragmentStartedNanos;
    BinaryHandler(WebSocketClientHandshaker handshaker, WebSocketMessageHandler handler, long token) {
      super(handshaker, handler);
      this.token = token;
    }
    private boolean current() {
      return !closed && !isTerminal() && token == generation.get()
          && activeHandler == this && activeChannel == context.channel();
    }
    @Override public void handlerAdded(ChannelHandlerContext ctx) {
      super.handlerAdded(ctx);
      context = ctx;
      boolean accepted;
      synchronized (stateGate) {
        accepted = !isTerminal() && token == generation.get();
        if (accepted) {
          // A successor gets its own silence allowance even before its first handshake response.
          lastIngressNanos = System.nanoTime();
          activeChannel = ctx.channel();
          activeHandler = this;
        }
      }
      if (!accepted) { ctx.close(); return; }
      ctx.channel().config().setAutoRead(false);
      ctx.pipeline().addBefore(ctx.name(), "p2p-ingress", new IngressHandler(this));
      ctx.pipeline().addBefore(ctx.name(), "p2p-flow", new FlowControlHandler(true) {
        @Override public void handlerRemoved(ChannelHandlerContext flowContext) throws Exception {
          // Netty drains downstream on removal. Keep the fenced consumer installed until it
          // has released every envelope; closing first can tear down that consumer mid-drain.
          closed = true;
          try {
            super.handlerRemoved(flowContext);
          } finally {
            closeGeneration();
          }
        }
      });
      SslHandler ssl = ctx.pipeline().get(SslHandler.class);
      if (ssl != null) {
        SSLParameters parameters = ssl.engine().getSSLParameters();
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        ssl.engine().setSSLParameters(parameters);
      }
      if (ctx.channel().isActive()) requestNext();
    }
    @Override public void channelActive(ChannelHandlerContext ctx) {
      super.channelActive(ctx);
      requestNext();
    }
    /** Event-loop only. Scheduling avoids recursive read/dequeue chains for control and fragments. */
    private void requestNext() {
      if (!current() || !context.channel().isActive() || handoff || readPending || demandScheduled) return;
      demandScheduled = true;
      try {
        context.executor().execute(() -> {
          demandScheduled = false;
          if (!current() || !context.channel().isActive() || handoff || readPending) return;
          readPending = true;
          context.read();
        });
      } catch (RejectedExecutionException error) {
        demandScheduled = false;
        invalidate(token, new IOException("P2P read resume rejected", error), false);
        closeGeneration();
      }
    }
    private void completeHandoff(List<IngressCredit> owned) {
      for (IngressCredit credit : owned) credit.release();
      try {
        context.executor().execute(() -> {
          handoff = false;
          requestNext();
        });
      } catch (RejectedExecutionException error) {
        // No event-loop work is possible. Fence this handler; never resume a replacement channel.
        closed = true;
        invalidate(token, new IOException("P2P handoff completion rejected", error), false);
        context.channel().close();
      }
    }
    private void releaseFragments() {
      fragments = null;
      fragmentStartedNanos = 0;
      for (IngressCredit credit : fragmentCredits) credit.release();
      fragmentCredits.clear();
    }
    private void closeGeneration() {
      closed = true;
      readPending = false;
      releaseFragments();
      if (context != null) context.close();
    }
    @Override public void channelRead0(ChannelHandlerContext ctx, Object received) throws Exception {
      readPending = false;
      if (!current()) {
        // Flow removal owns the eventual close; a reentrant close here would interrupt its drain.
        if (!closed) ctx.close();
        return;
      }
      if (!(received instanceof IngressMessage)) {
        invalidate(token, new IOException("P2P ingress admission bypassed"), true);
        return;
      }
      IngressMessage envelope = (IngressMessage) received;
      Object message = envelope.message;
      long nanos = envelope.credit.nanos;
      long micros = envelope.credit.micros;
      try {
        if (System.nanoTime() - nanos > MAX_QUEUE_AGE_NANOS) {
          invalidate(token, new IOException("P2P ingress queue age limit"), false);
          return;
        }
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
        if ((continuation && (fragments == null || System.nanoTime() - fragmentNanos > MAX_QUEUE_AGE_NANOS))
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
          releaseFragments();
          invalidate(token, new IOException("Fragment size limit"), true);
          return;
        }
        byte[] copy = new byte[length];
        frame.content().getBytes(frame.content().readerIndex(), copy);
        fragmentCredits.add(envelope.transfer());
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
        final byte[] payload = copy;
        final boolean binaryMessage = binary;
        final long ingressMicros = fragmentMicros;
        final long ingressNanos = fragmentNanos;
        List<IngressCredit> owned = new ArrayList<>(fragmentCredits);
        fragmentCredits.clear();
        handoff = true;
        // Admission already reserved the payload. Transfer that reservation, do not reserve twice.
        enqueue(token, 0, () -> {
          if (binaryMessage) {
            long decodeStartedNanos = System.nanoTime();
            process(codec.decode(payload), payload.length, ingressMicros, ingressNanos, decodeStartedNanos);
          } else {
            String text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(payload)).toString();
            control(text, token);
          }
        }, ingressNanos, () -> completeHandoff(owned));
      } finally {
        if (!handoff) requestNext(); // Handshake, ping/pong and nonfinal fragments created no Task.
      }
    }
    @Override public void channelInactive(ChannelHandlerContext ctx) {
      closed = true;
      readPending = false;
      releaseFragments();
      invalidate(token, new IOException("P2P socket closed"), false);
      super.channelInactive(ctx); // Keep native reconnect ownership and stale-channel fences.
    }
    @Override public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
      invalidate(token, new IOException("P2P ingress handler removed"), false);
      closeGeneration();
      super.handlerRemoved(ctx);
    }
    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable error) {
      releaseFragments();
      invalidate(token, error, true);
      if (!handshakeFuture().isDone()) ((io.netty.channel.ChannelPromise) handshakeFuture()).tryFailure(error);
      closeGeneration();
    }
  }

  interface SnapshotSource {
    P2pWireCodec.Snapshot fetch(Set<String> coins, BigInteger minHeight) throws Exception;
  }
  private interface Work { void run() throws Exception; }
  private static final class BookProvenance {
    final BigInteger createdAtMicros;
    final Cursor cursor;
    BookProvenance(BigInteger createdAtMicros, Cursor cursor) {
      this.createdAtMicros = createdAtMicros;
      this.cursor = cursor;
    }
  }
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
  private static final class PartialTrade {
    final BigInteger height;
    final long ordinal;
    final byte[] fingerprint;
    int emittedPrefix;
    PartialTrade(BigInteger height, long ordinal, byte[] fingerprint) {
      this.height = height; this.ordinal = ordinal; this.fingerprint = fingerprint;
    }
  }

  public static final class TradePublication {
    public final String id;
    public final P2pWireCodec.TradeEvent event;
    private TradePublication(String id, P2pWireCodec.TradeEvent event) {
      this.id = id; this.event = event;
    }
    public String getId() { return id; }
    public P2pWireCodec.TradeEvent getEvent() { return event; }
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
