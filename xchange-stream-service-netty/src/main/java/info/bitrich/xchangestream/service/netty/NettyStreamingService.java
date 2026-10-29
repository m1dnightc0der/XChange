package info.bitrich.xchangestream.service.netty;

import com.fasterxml.jackson.core.JsonProcessingException;
import info.bitrich.xchangestream.service.ConnectableService;
import info.bitrich.xchangestream.service.exception.NotConnectedException;
import info.bitrich.xchangestream.service.netty.ConnectionStateModel.State;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.codec.http.websocketx.extensions.WebSocketClientExtensionHandler;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.proxy.Socks5ProxyHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.internal.SocketUtils;
import io.netty.util.internal.StringUtil;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.reactivex.rxjava3.disposables.SerialDisposable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public abstract class NettyStreamingService<T> extends ConnectableService {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    protected static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(5);
    protected static final Duration DEFAULT_RETRY_DURATION = Duration.ofSeconds(5);
    protected static final int DEFAULT_IDLE_TIMEOUT = 5;
    public static final String LATENCY_PROBE_PROPERTY = "xchangestream.latencyProbe";
    private static final AtomicLong DISPATCHER_THREAD_COUNTER = new AtomicLong();
    private static final boolean LATENCY_PROBE = Boolean.getBoolean(LATENCY_PROBE_PROPERTY);

    protected class Subscription {

        final ObservableEmitter<T> emitter;
        final String channelName;
        final Object[] args;
        final ExecutorService dispatcher;

        public Subscription(ObservableEmitter<T> emitter, String channelName, Object[] args) {
            this.emitter = emitter;
            this.channelName = channelName;
            this.args = args;
            this.dispatcher =
                    Executors.newSingleThreadExecutor(
                            runnable -> {
                                Thread thread =
                                        new Thread(
                                                runnable,
                                                "xchange-stream-dispatch-"
                                                        + DISPATCHER_THREAD_COUNTER.incrementAndGet()
                                                        + "-"
                                                        + channelName);
                                thread.setDaemon(true);
                                return thread;
                            });
        }

        public ObservableEmitter<T> getEmitter() {
            return emitter;
        }

        public void onNext(T message, long enqueueMs, Long exchangeMs, String messageHash) {
            try {
                dispatcher.execute(
                        () -> {
                            long subscriberStartMs = System.currentTimeMillis();
                            if (LATENCY_PROBE) {
                                LOG.info(
                                        "XCHANGE_STREAM_LATENCY_PROBE stage=subscriber_onnext_start channel={} exchange_ms={} enqueue_ms={} subscriber_start_ms={} enqueue_to_subscriber_ms={} exchange_to_subscriber_ms={} message_hash={} thread={}",
                                        channelName,
                                        exchangeMs,
                                        enqueueMs,
                                        subscriberStartMs,
                                        subscriberStartMs - enqueueMs,
                                        exchangeMs == null ? null : subscriberStartMs - exchangeMs,
                                        messageHash,
                                        Thread.currentThread().getName());
                            }
                            if (emitter.isDisposed()) {
                                return;
                            }
                            try {
                                emitter.onNext(message);
                            } catch (Throwable t) {
                                if (!emitter.isDisposed()) {
                                    emitter.onError(t);
                                }
                            }
                        });
            } catch (RejectedExecutionException e) {
                LOG.debug("Dispatcher has been closed for channel {}.", channelName);
            }
        }

        public void dispose() {
            dispatcher.shutdownNow();
        }
    }

    private final int maxFramePayloadLength;
    protected URI uri;
    private final AtomicBoolean isManualDisconnect = new AtomicBoolean();
    private volatile Channel webSocketChannel;
    private final Object reconnectLock = new Object();
    private CompletableFuture<Void> reconnectFuture;
    private ScheduledFuture<?> reconnectTask;
    private SerialDisposable reconnectSubscription;
    private long connectionGeneration;
    private final Duration retryDuration;
    private final Duration connectionTimeout;
    private final int idleTimeoutSeconds;
    private volatile NioEventLoopGroup eventLoopGroup;
    protected final Map<String, Subscription> channels = new ConcurrentHashMap<>();
    volatile Instant retry = null;

    protected final Map<String, ObservableEmitter<T>> singles = new ConcurrentHashMap<>();
    private boolean compressedMessages = false;

    private final Subject<Throwable> reconnFailEmitters = PublishSubject.create();
    private final Subject<Object> connectionSuccessEmitters = PublishSubject.create();
    private final Subject<Object> disconnectEmitters = PublishSubject.create();
    private final Subject<Object> subjectIdle = PublishSubject.create();

    private final ConnectionStateModel connectionStateModel = new ConnectionStateModel();

    // debugging
    private boolean acceptAllCertificates = false;
    private boolean enableLoggingHandler = false;
    private volatile boolean autoReconnect = true;
    private LogLevel loggingHandlerLevel = LogLevel.DEBUG;
    private String socksProxyHost;
    private Integer socksProxyPort;

    public NettyStreamingService(String apiUrl) {
        this(apiUrl, 65536);
    }

    public NettyStreamingService(String apiUrl, int maxFramePayloadLength) {
        this(apiUrl, maxFramePayloadLength, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_RETRY_DURATION);
    }

    public NettyStreamingService(
            String apiUrl,
            int maxFramePayloadLength,
            Duration connectionTimeout,
            Duration retryDuration) {
        this(apiUrl, maxFramePayloadLength, connectionTimeout, retryDuration, DEFAULT_IDLE_TIMEOUT);
    }

    public NettyStreamingService(
            String apiUrl,
            int maxFramePayloadLength,
            Duration connectionTimeout,
            Duration retryDuration,
            int idleTimeoutSeconds) {
        this.maxFramePayloadLength = maxFramePayloadLength;
        this.retryDuration = retryDuration;
        this.connectionTimeout = connectionTimeout;
        this.idleTimeoutSeconds = idleTimeoutSeconds;
        try {
            this.uri = new URI(apiUrl);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Error parsing URI " + apiUrl, e);
        }
    }

    @Override
    protected Completable openConnection() {
        return Completable.defer(() -> {
            final long generation;
            final boolean recovering;
            synchronized (reconnectLock) {
                // Retry admission is fenced under this same lock. Only a new, subscribed
                // connection can reset manual suppression; old timers cannot get here.
                isManualDisconnect.set(false);
                generation = ++connectionGeneration;
                recovering = reconnectFuture != null && !reconnectFuture.isDone();
                if (eventLoopGroup == null || eventLoopGroup.isShuttingDown()) {
                    eventLoopGroup = new NioEventLoopGroup(3);
                }
            }
            return Completable.create(completable -> {
                synchronized (reconnectLock) {
                    if (isManualDisconnect.get() || generation != connectionGeneration) {
                        completable.tryOnError(new CancellationException("Connection cancelled"));
                        return;
                    }
                    LOG.info("Connecting to {}", uri.toString());
                    String scheme = uri.getScheme() == null ? "ws" : uri.getScheme();
                    String host = uri.getHost();
                    if (host == null) {
                        throw new IllegalArgumentException("Host cannot be null.");
                    }
                    if (!"ws".equalsIgnoreCase(scheme) && !"wss".equalsIgnoreCase(scheme)) {
                        throw new IllegalArgumentException("Only WS(S) is supported.");
                    }
                    final boolean ssl = "wss".equalsIgnoreCase(scheme);
                    final int port = uri.getPort() == -1 ? (ssl ? 443 : 80) : uri.getPort();
                    final SslContext sslCtx;
                    if (ssl) {
                        SslContextBuilder builder = SslContextBuilder.forClient();
                        if (acceptAllCertificates) {
                            builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                        }
                        sslCtx = builder.build();
                    } else {
                        sslCtx = null;
                    }
                    final WebSocketClientHandler handler = getWebSocketClientHandler(
                            WebSocketClientHandshakerFactory.newHandshaker(
                                    uri, WebSocketVersion.V13, null, true,
                                    getCustomHeaders(), maxFramePayloadLength),
                            this::messageHandler);
                    ChannelFuture transport = new Bootstrap()
                            .group(eventLoopGroup)
                            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                                    Math.toIntExact(connectionTimeout.toMillis()))
                            .option(ChannelOption.SO_KEEPALIVE, true)
                            .channel(NioSocketChannel.class)
                            .handler(new ChannelInitializer<SocketChannel>() {
                                @Override
                                protected void initChannel(SocketChannel ch) {
                                    ChannelPipeline p = ch.pipeline();
                                    if (socksProxyHost != null) {
                                        p.addLast(new Socks5ProxyHandler(
                                                SocketUtils.socketAddress(socksProxyHost, socksProxyPort)));
                                    }
                                    if (sslCtx != null) {
                                        p.addLast(sslCtx.newHandler(ch.alloc(), host, port));
                                    }
                                    p.addLast(new HttpClientCodec());
                                    if (enableLoggingHandler) {
                                        p.addLast(new LoggingHandler(loggingHandlerLevel));
                                    }
                                    if (compressedMessages) {
                                        p.addLast(WebSocketClientCompressionHandler.INSTANCE);
                                    }
                                    p.addLast(new HttpObjectAggregator(8192));
                                    if (idleTimeoutSeconds > 0) {
                                        p.addLast(new IdleStateHandler(idleTimeoutSeconds, 0, 0));
                                    }
                                    WebSocketClientExtensionHandler extension = getWebSocketClientExtensionHandler();
                                    if (extension != null) {
                                        p.addLast(extension);
                                    }
                                    p.addLast(handler);
                                }
                            }).connect(host, port);
                    final Channel attemptChannel = transport.channel();
                    webSocketChannel = attemptChannel;
                    // A peer can close before replying to the upgrade. Terminate that attempt too.
                    attemptChannel.closeFuture().addListener(ignored ->
                            completable.tryOnError(new ClosedChannelException()));
                    transport.addListener((ChannelFuture connected) -> {
                        synchronized (reconnectLock) {
                            if (isManualDisconnect.get() || generation != connectionGeneration
                                    || completable.isDisposed()) {
                                attemptChannel.close();
                                completable.tryOnError(new CancellationException("Connection cancelled"));
                            } else if (!connected.isSuccess()) {
                                completable.tryOnError(connected.cause());
                                attemptChannel.close();
                            } else {
                                handler.handshakeFuture().addListener(handshake -> {
                                    synchronized (reconnectLock) {
                                        if (isManualDisconnect.get() || generation != connectionGeneration
                                                || completable.isDisposed()) {
                                            attemptChannel.close();
                                            completable.tryOnError(new CancellationException("Connection cancelled"));
                                        } else if (handshake.isSuccess()) {
                                            connectionStateModel.setState(State.OPEN);
                                            completable.onComplete();
                                        } else {
                                            completable.tryOnError(handshake.cause());
                                            attemptChannel.close();
                                        }
                                    }
                                });
                            }
                        }
                    });
                }
            }).doOnError(error -> {
                synchronized (reconnectLock) {
                    if (generation != connectionGeneration || isManualDisconnect.get()) {
                        return;
                    }
                    connectionStateModel.setState(State.CLOSED);
                    LOG.warn("Connection failed service={} role={} generation={} cause={}",
                            getClass().getSimpleName(), connectionRole(), generation,
                            error.getClass().getSimpleName());
                    reconnFailEmitters.onNext(error);
                    scheduleReconnect();
                }
            }).doOnComplete(() -> {
                synchronized (reconnectLock) {
                    // Automatic recovery must wait for the complete overridden connect()
                    // chain (including exchange authentication hooks) before replaying.
                    if (!recovering && generation == connectionGeneration
                            && !isManualDisconnect.get() && !channels.isEmpty()) {
                        resubscribeChannels();
                        connectionSuccessEmitters.onNext(new Object());
                    }
                }
            });
        });
    }

    private CompletableFuture<Void> scheduleReconnect() {
        synchronized (reconnectLock) {
            if (reconnectFuture != null && !reconnectFuture.isDone()) {
                return reconnectFuture;
            }
            CompletableFuture<Void> future = new CompletableFuture<>();
            if (!autoReconnect || isManualDisconnect.get()) {
                future.cancel(false);
                return future;
            }
            reconnectFuture = future;
            setRetryDelay(retryDuration);
            scheduleReconnectWithRetry(future);
            return future;
        }
    }

    private boolean canRetry(CompletableFuture<Void> future) {
        if (future != reconnectFuture || future.isDone()) {
            return false;
        }
        if (!autoReconnect || isManualDisconnect.get()
                || eventLoopGroup == null || eventLoopGroup.isShuttingDown()) {
            future.cancel(false);
            return false;
        }
        return true;
    }

    private void scheduleReconnectWithRetry(CompletableFuture<Void> future) {
        synchronized (reconnectLock) {
            if (!canRetry(future)) {
                return;
            }
            long delay = retry == null ? retryDuration.toMillis()
                    : Math.max(1, Duration.between(Instant.now(), retry).toMillis());
            LOG.info("Reconnect scheduled service={} role={} generation={} nextRetryMs={}",
                    getClass().getSimpleName(), connectionRole(), connectionGeneration, delay);
            try {
                reconnectTask = eventLoopGroup.next().schedule(() -> {
                    synchronized (reconnectLock) {
                        if (!canRetry(future)) {
                            return;
                        }
                        if (retry != null && retry.isAfter(Instant.now())) {
                            scheduleReconnectWithRetry(future);
                            return;
                        }
                        retry = null;
                        SerialDisposable attempt = new SerialDisposable();
                        reconnectSubscription = attempt;
                        attempt.set(Completable.defer(this::connect)
                                .andThen(Completable.fromAction(() -> {
                                    synchronized (reconnectLock) {
                                        if (!canRetry(future) || !isSocketOpen()) {
                                            throw new CancellationException("Recovery connection closed");
                                        }
                                        resubscribeChannels();
                                        // A close can race an asynchronous exchange completion hook.
                                        // Replay and owner completion are one critical section: an
                                        // inactive callback must either join a retrying owner or start
                                        // a new one, never join an owner about to falsely succeed.
                                        if (!isSocketOpen()) {
                                            throw new ClosedChannelException();
                                        }
                                        LOG.info("Resubscription complete service={} role={} generation={} channels={}",
                                                getClass().getSimpleName(), connectionRole(),
                                                connectionGeneration, channels.size());
                                        connectionSuccessEmitters.onNext(new Object());
                                        future.complete(null);
                                    }
                                }))
                                .subscribe(() -> {}, error -> {
                                    synchronized (reconnectLock) {
                                        if (!canRetry(future)) {
                                            return;
                                        }
                                        LOG.warn("Recovery failed service={} role={} generation={} cause={}",
                                                getClass().getSimpleName(), connectionRole(),
                                                connectionGeneration, error.getClass().getSimpleName());
                                        if (webSocketChannel != null) {
                                            webSocketChannel.close();
                                        }
                                        setRetryDelay(retryDuration);
                                        scheduleReconnectWithRetry(future);
                                    }
                                }));
                    }
                }, delay, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException error) {
                future.completeExceptionally(error);
                LOG.warn("Reconnect scheduling rejected service={} role={} generation={}",
                        getClass().getSimpleName(), connectionRole(), connectionGeneration);
            }
        }
    }

    /**
     * Sets a minimum delay before the next reconnection attempt.
     * Useful for subclasses to enforce rate limits (e.g., OKX's 3 connections/second limit).
     *
     * @param delay the minimum duration to wait before reconnecting
     */
    protected void setRetryDelay(Duration delay) {
        Instant newRetry = Instant.now().plus(delay);
        Instant currentRetry = this.retry;
        if (currentRetry == null || newRetry.isAfter(currentRetry)) {
            this.retry = newRetry;
        }
    }

    private String connectionRole() {
        String path = uri.getPath();
        if (path != null && path.endsWith("/public")) {
            return "public";
        }
        if (path != null && path.endsWith("/private")) {
            return "private";
        }
        // Shared endpoints (including HL) do not identify a role at this layer.
        return "unknown";
    }

    protected DefaultHttpHeaders getCustomHeaders() {
        return new DefaultHttpHeaders();
    }

    private void clearChannels() {
        channels.values().forEach(Subscription::dispose);
        channels.clear();
    }

    // Called under reconnectLock, before shutting down or closing an in-flight attempt.
    private void cancelReconnect() {
        ++connectionGeneration;
        if (reconnectTask != null) {
            reconnectTask.cancel(false);
        }
        if (reconnectSubscription != null) {
            reconnectSubscription.dispose();
        }
        if (reconnectFuture != null) {
            reconnectFuture.cancel(false);
        }
    }

    public Completable disconnect() {
        synchronized (reconnectLock) {
            isManualDisconnect.set(true);
            cancelReconnect();
        }
        return Completable.create(
                completable -> {
                    if (webSocketChannel != null && webSocketChannel.isOpen()) {
                        CloseWebSocketFrame closeFrame = new CloseWebSocketFrame();
                        webSocketChannel
                                .writeAndFlush(closeFrame)
                                .addListener(
                                        future -> {
                                            clearChannels();
                                            eventLoopGroup
                                                    .shutdownGracefully(2, idleTimeoutSeconds, TimeUnit.SECONDS)
                                                    .addListener(
                                                            f -> {
                                                                LOG.debug("Disconnected");
                                                                connectionStateModel.setState(State.CLOSED);
                                                                disconnectEmitters.onNext(new Object());
                                                                completable.onComplete();
                                                            });
                                        });
                    } else if (webSocketChannel != null) { // web socket is closed already
                        clearChannels();
                        eventLoopGroup
                                .shutdownGracefully(2, idleTimeoutSeconds, TimeUnit.SECONDS)
                                .addListener(
                                        f -> {
                                            connectionStateModel.setState(State.CLOSED);
                                            disconnectEmitters.onNext(new Object());
                                            completable.onComplete();
                                        });
                    } else {
                        clearChannels();
                        connectionStateModel.setState(State.CLOSED);
                        NioEventLoopGroup group = eventLoopGroup;
                        if (group == null) {
                            completable.onComplete();
                        } else {
                            // Setup can fail after allocating loops but before creating a channel.
                            group.shutdownGracefully(0, idleTimeoutSeconds, TimeUnit.SECONDS)
                                    .addListener(ignored -> completable.onComplete());
                        }
                    }
                });
    }

    protected abstract String getChannelNameFromMessage(T message) throws IOException;

    public abstract String getSubscribeMessage(String channelName, Object... args) throws IOException;

    public abstract String getUnsubscribeMessage(String channelName, Object... args)
            throws IOException;

    public String getSubscriptionUniqueId(String channelName, Object... args) {

        if (args == null || args.length == 0) {
            return channelName;
        }

        List<String> collect = Arrays.stream(args).map(String::valueOf).collect(Collectors.toList());
        String argsString = String.join("-", collect);

        return channelName + "_" + argsString;
    }

    /**
     * Handler that receives incoming messages.
     *
     * @param message Content of the message from the server.
     */
    public abstract void messageHandler(String message);

    protected String sanitizeMessageForLogging(String message) {
        return message;
    }

    public synchronized ChannelFuture sendMessage(String message) {
        String loggedMessage = sanitizeMessageForLogging(message);
        LOG.debug("Sending message: {} called from {}", loggedMessage, Thread.currentThread().getStackTrace()[2]);
        if (message != null && !message.contains("ping")) {
            LOG.debug("Non ping message: {}", loggedMessage);
        }
        if (!isSocketOpen()) {
            LOG.debug("WebSocket is not open! Call connect first. Unable to send message "+loggedMessage+" called from:"+Thread.currentThread().getStackTrace()[2]);
            return null;
        }

        if (!webSocketChannel.isWritable()) {
            LOG.debug("Cannot send data to WebSocket as it is not writable. Unable to send message "+loggedMessage+" called from:"+Thread.currentThread().getStackTrace()[2]);
            return null;
        }
        if (message != null) {

            return webSocketChannel.writeAndFlush(new TextWebSocketFrame(message));


        }
        return null;
    }

    public Observable<Throwable> subscribeReconnectFailure() {
        return reconnFailEmitters.share();
    }

    public Observable<Object> subscribeConnectionSuccess() {
        return connectionSuccessEmitters.share();
    }

    public Observable<Object> subscribeDisconnect() {
        return disconnectEmitters.share();
    }

    public Observable<State> subscribeConnectionState() {
        return connectionStateModel.stateObservable();
    }

/*    public Single<T> sendMessageAndSubscribeChannel(String channelName, String message, Object... args) {
        final String subscriptionUniqueId = getSubscriptionUniqueId(channelName, args);
        LOG.debug("subscribeChannel : called from {}", Thread.currentThread().getStackTrace()[2]);
        LOG.info("Subscribing to subscriptionUniqueId={}, args={}", subscriptionUniqueId, args);

        return Single.<T>create(
                        e -> {
                            if (webSocketChannel == null || !webSocketChannel.isOpen()) {
                                e.onError(new NotConnectedException());
                                return;
                            }
                            channels.computeIfAbsent(
                                    subscriptionUniqueId,
                                    cid -> {
                                        Subscription newSubscription = new Subscription(e, channelName, args);
                                        try {
                                            sendMessage(message);
                                        } catch (
                                                Exception
                                                        throwable) { // if getSubscribeMessage throws this, it is because it
                                            // needs to report
                                            e.onError(throwable); // a problem creating the message
                                        }
                                        return newSubscription;
                                    });
                        })
                .doOnDispose(
                        () -> {
                           channels.remove(subscriptionUniqueId);
                        })
                .share();
    }*/

    public @NonNull Observable<T> subscribeSingle(Long id, String message) {
        LOG.debug("subscribeSingle to id={}, message={}", id, message);


        return Observable.<T>create(
                        e -> {
                            if (webSocketChannel == null || !webSocketChannel.isOpen()) {
                                e.onError(new NotConnectedException());
                                return;
                            }
                            singles.computeIfAbsent(
                                    id.toString(),
                                    cid -> {
                                        sendMessage(message);

                                        return e;
                                    });
                        })
                .doOnDispose(
                        () -> {
                            singles.remove(id.toString());


                        });
    }

    public Observable<T> subscribeChannel(String channelName, Object... args) {
        final String subscriptionUniqueId = getSubscriptionUniqueId(channelName, args);
         LOG.info("Subscribing to subscriptionUniqueId={}, args={}", subscriptionUniqueId, args);


        return Observable.<T>create(
                        e -> {
                            if (webSocketChannel == null || !webSocketChannel.isOpen()) {
                                e.onError(new NotConnectedException());
                                return;
                            }
                            channels.computeIfAbsent(
                                    subscriptionUniqueId,
                                    cid -> {
                                        Subscription newSubscription = new Subscription(e, channelName, args);
                                        try {
                                            sendMessage(getSubscribeMessage(channelName, args));
                                        } catch (
                                                Exception
                                                        throwable) { // if getSubscribeMessage throws this, it is because it
                                            // needs to report
                                            e.onError(throwable); // a problem creating the message
                                        }
                                        return newSubscription;
                                    });
                        })
                .doOnDispose(
                        () -> {
                            Subscription subscription = channels.remove(subscriptionUniqueId);
                            if (subscription != null) {
                                subscription.dispose();
                                try {
                                    sendMessage(getUnsubscribeMessage(subscriptionUniqueId, args));
                                } catch (IOException e) {
                                    LOG.error("Failed to unsubscribe channel: {} {}", subscriptionUniqueId, e.toString());
                                } catch (Exception e) {
                                    LOG.error("Failed to unsubscribe channel: {}", subscriptionUniqueId, e);
                                }
                            }
                        })
                .share();
    }

    public void resubscribeChannels() throws IOException {
         for (Entry<String, Subscription> entry : channels.entrySet()) {
            try {
                Subscription subscription = entry.getValue();
                //These need to be rate limited.
                sendMessage(getSubscribeMessage(subscription.channelName, subscription.args));
                try {
                    Thread.sleep(this.retryDuration.toMillis() / 100);
                } catch (Exception e) {

                }
            } catch (Exception e) {
                LOG.error("Failed to reconnect channel: {}", entry.getKey());
                throw e;
            }
        }
    }

    public void resubscribeChannel(String channelName) throws IOException {
        try {
            Subscription subscription = channels.get(channelName);
            sendMessage(getSubscribeMessage(subscription.channelName, subscription.args));
        } catch (Exception e) {
            LOG.error("Failed to reconnect channel: {}", channelName);
            throw e;
        }
    }


    protected String getChannel(T message) {
        String channel;
        try {
            channel = getChannelNameFromMessage(message);
        } catch (IOException e) {
            LOG.error("Cannot parse channel from message: {}", message);
            return "";
        }
        return channel;
    }

    protected void handleMessage(T message) {
        String channel = getChannel(message);
        if (!StringUtil.isNullOrEmpty(channel)) {
            handleChannelMessage(channel, message);
        }
    }

    protected Long latencyProbeExchangeTimestampMillis(T message) {
        return null;
    }

    protected String latencyProbeMessageHash(T message) {
        return Integer.toHexString(System.identityHashCode(message));
    }

    protected void handleError(T message, Throwable t) {
        String channel = getChannel(message);
        if (!StringUtil.isNullOrEmpty(channel)) {
            handleChannelError(channel, t);
        } else {
            LOG.error("handleError cannot parse channel from message: {}", message);
        }
    }

    protected void handleIdle(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(new PingWebSocketFrame());
    }

    private void onIdle(ChannelHandlerContext ctx) {
        subjectIdle.onNext(1);
        handleIdle(ctx);
    }

    /**
     * Observable which fires if the websocket is deemed idle, only fired if <code>
     * idleTimeoutSeconds != 0</code>.
     */
    public Observable<Object> subscribeIdle() {
        return subjectIdle.share();
    }

    protected void handleChannelMessage(String channel, T message) {
        if (channel == null) {
            LOG.debug("Channel provided is null");
            return;
        }
        NettyStreamingService<T>.Subscription subscription = channels.get(channel);
        if (subscription == null) {
            LOG.debug("Channel has been closed {}.", channel);
            return;
        }
        ObservableEmitter<T> emitter = subscription.emitter;
        if (emitter == null) {
            LOG.debug("No subscriber for channel {}.", channel);
            return;
        }
        long enqueueMs = System.currentTimeMillis();
        Long exchangeMs = LATENCY_PROBE ? latencyProbeExchangeTimestampMillis(message) : null;
        String messageHash = LATENCY_PROBE ? latencyProbeMessageHash(message) : null;
        if (LATENCY_PROBE) {
            LOG.info(
                    "XCHANGE_STREAM_LATENCY_PROBE stage=channel_enqueue channel={} exchange_ms={} enqueue_ms={} exchange_to_enqueue_ms={} message_hash={}",
                    channel,
                    exchangeMs,
                    enqueueMs,
                    exchangeMs == null ? null : enqueueMs - exchangeMs,
                    messageHash);
        }
        subscription.onNext(message, enqueueMs, exchangeMs, messageHash);
    }

    protected void handleChannelError(String channel, Throwable t) {
        NettyStreamingService<T>.Subscription subscription = channels.get(channel);
        if (subscription == null) {
            LOG.debug("Channel {} has been closed.", channel);
            return;
        }
        ObservableEmitter<T> emitter = subscription.emitter;
        if (emitter == null) {
            LOG.debug("No subscriber for channel {}.", channel);
            return;
        }

        emitter.onError(t);
    }

    protected WebSocketClientExtensionHandler getWebSocketClientExtensionHandler() {
        return WebSocketClientCompressionHandler.INSTANCE;
    }

    protected WebSocketClientHandler getWebSocketClientHandler(
            WebSocketClientHandshaker handshaker,
            WebSocketClientHandler.WebSocketMessageHandler handler) {
        return new NettyWebSocketClientHandler(handshaker, handler);
    }

    protected class NettyWebSocketClientHandler extends WebSocketClientHandler {

        protected NettyWebSocketClientHandler(
                WebSocketClientHandshaker handshaker, WebSocketMessageHandler handler) {
            super(handshaker, handler);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            super.channelInactive(ctx);
            synchronized (reconnectLock) {
                if (isManualDisconnect.get() || ctx.channel() != webSocketChannel) {
                    return;
                }
                connectionStateModel.setState(State.CLOSED);
                disconnectEmitters.onNext(new Object());
                scheduleReconnect();
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (!(evt instanceof IdleStateEvent)) {
                return;
            }
            IdleStateEvent e = (IdleStateEvent) evt;
            if (e.state().equals(IdleState.READER_IDLE)) {
                onIdle(ctx);
            }
        }
    }

    public boolean isSocketOpen() {
        return webSocketChannel != null
                && webSocketChannel.isOpen()
                && webSocketChannel.isWritable()
                && connectionStateModel.getState() == State.OPEN;
    }

    public void useCompressedMessages(boolean compressedMessages) {
        this.compressedMessages = compressedMessages;
    }

    public void setAcceptAllCertificates(boolean acceptAllCertificates) {
        this.acceptAllCertificates = acceptAllCertificates;
    }

    public void setEnableLoggingHandler(boolean enableLoggingHandler) {
        this.enableLoggingHandler = enableLoggingHandler;
    }

    public void setLoggingHandlerLevel(LogLevel loggingHandlerLevel) {
        this.loggingHandlerLevel = loggingHandlerLevel;
    }

    public void setSocksProxyHost(String socksProxyHost) {
        this.socksProxyHost = socksProxyHost;
    }

    public void setSocksProxyPort(Integer socksProxyPort) {
        this.socksProxyPort = socksProxyPort;
    }

    public void setAutoReconnect(boolean autoReconnect) {
        synchronized (reconnectLock) {
            this.autoReconnect = autoReconnect;
            if (!autoReconnect && reconnectFuture != null && !reconnectFuture.isDone()) {
                cancelReconnect();
                connectionStateModel.setState(State.CLOSED);
                if (webSocketChannel != null) {
                    webSocketChannel.close();
                }
            }
        }
    }
}
