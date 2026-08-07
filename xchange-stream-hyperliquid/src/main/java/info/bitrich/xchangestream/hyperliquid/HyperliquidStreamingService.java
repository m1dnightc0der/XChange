package info.bitrich.xchangestream.hyperliquid;

import com.fasterxml.jackson.databind.JsonNode;
import info.bitrich.xchangestream.hyperliquid.dto.HyperliquidPingMessage;
import info.bitrich.xchangestream.hyperliquid.dto.HyperliquidSubscribeMessage;
import info.bitrich.xchangestream.service.netty.JsonNettyStreamingService;
import info.bitrich.xchangestream.service.netty.WebSocketClientHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.CompletableSource;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.reactivex.rxjava3.disposables.Disposable;
import org.knowm.xchange.ExchangeSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class HyperliquidStreamingService extends JsonNettyStreamingService {

    private static final Logger LOG = LoggerFactory.getLogger(HyperliquidStreamingService.class);
    public boolean isLoggedIn = false;
    private static final String SUBSCRIBE = "subscribe";

    private static final String PING = "ping";
    private static final String UNSUBSCRIBE = "unsubscribe";

    private final Observable<Long> pingPongSrc = Observable.interval(15, 15, TimeUnit.SECONDS);

    private Disposable pingPongSubscription;
    public static final String BBO = "bbo";
    public static final String L2BOOK = "l2Book";
    public static final String TRADES = "trades";

    private WebSocketClientHandler.WebSocketMessageHandler channelInactiveHandler = null;
    private final ExchangeSpecification exchangeSpecification;
    private final AtomicLong connectGeneration = new AtomicLong();

    public HyperliquidStreamingService(String apiUrl, ExchangeSpecification exchangeSpecification) {
        super(apiUrl);
        this.exchangeSpecification = exchangeSpecification;
    }
    public void pingPongDisconnectIfConnected() {
        if (pingPongSubscription != null && !pingPongSubscription.isDisposed()) {
            pingPongSubscription.dispose();
        }
    }

    @Override public Completable connect() {

        long generation = connectGeneration.incrementAndGet();
        LOG.debug("connect generation={} called from {}", generation, Thread.currentThread().getStackTrace()[2]);
        isLoggedIn=false;
        pingPongDisconnectIfConnected();
        Completable conn = super.connect();



        return conn.doFinally(() -> LOG.info("connect do finally")).andThen((CompletableSource) (completable) -> {
            try {


                if (pingPongSubscription != null && !pingPongSubscription.isDisposed()) {
                    pingPongSubscription.dispose();
                }
                if(!this.isSocketOpen()){
                    this.connect();
                }
                // FIX: Add state check and graceful error handling to prevent ping spam on closed sockets
                pingPongSubscription = pingPongSrc.subscribe(
                        msg -> {
                          // Only send ping if socket is open to avoid "WebSocket is not open!" warnings
                          if (isSocketOpen()) {
                            try {
                              this.sendMessage(getPingMessage());
                            } catch (Exception e) {
                              LOG.debug("Failed to send ping (socket may have closed): {}", e.getMessage());
                            }
                          } else {
                            LOG.debug("Skipping ping - socket not open");
                          }
                        },
                        error -> {
                          LOG.debug("Ping scheduler error: {}", error.getMessage());
                          completable.onError(error);
                        });
                completable.onComplete();
            } catch (Exception e) {
                completable.onError(e);
            }
        });
    }

    @Override
    public void messageHandler(String message) {
        LOG.debug("Received message: {}", message);
        JsonNode jsonNode = null;

        // Parse incoming message to JSON

        try {
            jsonNode = objectMapper.readTree(message);
        } catch (IOException e) {
            if ("pong".equals(message)) {
                // ping pong message
                return;
            }
            LOG.error("Error parsing incoming message to JSON: {}", message);
            return;
        }


        if (isSubscriptionAcknowledgement(jsonNode)) {
            JsonNode subscription = jsonNode.get("data").get("subscription");
            String channel = subscription.path("type").asText();
            String user = subscription.path("user").asText().toLowerCase(Locale.ROOT);
            LOG.info("privateStream event=subscription-ack exchange=HYPERLIQUID channel={} user={} connectGeneration={}",
                    channel, user, connectGeneration.get());
            handleMessage(jsonNode);
        } else if (jsonNode != null && jsonNode.has("channel") && !jsonNode.get("channel").asText().equals("post") && !jsonNode.get("channel").asText().equals("pong")) {
            handleMessage(jsonNode);
        } else if (jsonNode != null && jsonNode.has("channel") && jsonNode.get("channel").asText().equals("post") && jsonNode.has("data") && jsonNode.get("data").has("id")) {
            // Handle subscription responses
            ObservableEmitter<JsonNode> emitter = singles.get(jsonNode.get("data").get("id").asText());
            if (emitter == null) {
                LOG.debug("No emitter for single {}.", jsonNode.get("data").get("id"));
                return;
            }
            emitter.onNext(jsonNode);
            singles.remove(jsonNode.get("data").get("id").asText());
            emitter.onComplete();
        } else if (jsonNode != null && !jsonNode.get("channel").asText().equals("pong")) {
            handleMessage(jsonNode);
        }
    }

    private boolean isSubscriptionAcknowledgement(JsonNode message) {
        return message != null
                && "subscriptionResponse".equals(message.path("channel").asText())
                && message.path("data").path("subscription").has("type")
                && message.path("data").path("subscription").has("user");
    }

    @Override
    protected String getChannelNameFromMessage(JsonNode message) {


        String channelName = "";
        if (message.has("data") && message.get("data").has("subscription")) {

            if (message.get("data").get("subscription").has("type") && message.get("data").get("subscription").has("coin")) {
                channelName = message.get("data").get("subscription").get("type").asText() + "." + message.get("data").get("subscription").get("coin").asText();
            } else if (message.get("data").get("subscription").has("type") && message.get("data").get("subscription").has("user")) {
                // Handle user-specific channels (userFills, userEvents)
                channelName = message.get("data").get("subscription").get("type").asText() + "." + message.get("data").get("subscription").get("user").asText().toLowerCase(Locale.ROOT);
            }
        } else {
            if (message.has("channel") && message.has("data") && message.get("data").has("coin")) {
                channelName = message.get("channel").asText() + "." + message.get("data").get("coin").asText();
            } else if (message.has("channel") && message.has("data") &&  message.get("data").has(0) && message.get("data").get(0).has("coin")){
                channelName = message.get("channel").asText() + "." + message.get("data").get(0).get("coin").asText();
            } else if (message.has("channel") && message.has("data") && message.get("data").has("user")) {
                // Handle user-specific channel messages (userFills, userEvents)
                channelName = message.get("channel").asText() + "." + message.get("data").get("user").asText().toLowerCase();
            } else if (message.has("channel") && (message.get("channel").asText().equals("userEvents") || message.get("channel").asText().equals("userFills") || message.get("channel").asText().equals("orderUpdates"))) {
                if(exchangeSpecification!=null && exchangeSpecification.getExchangeSpecificParameters().get("wallet")!=null) {
                    channelName = message.get("channel").asText() + "."
                            + exchangeSpecification.getExchangeSpecificParameters().get("wallet")
                            .toString().toLowerCase(Locale.ROOT);
                } else{
                    channelName = message.get("channel").asText();
                }
            } else {
                channelName = message.get("channel").asText();
            }
        }

        return channelName;
    }

    @Override
    public String getSubscribeMessage(String channelName, Object... args) throws IOException {
        String[] parts = channelName.split("\\.");
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid channel name: " + channelName);
        }

        String type = parts[0];
        String identifier = parts[1]; // Can be coin or user address

        // For user-specific channels (userFills, userEvents), use "user" parameter
        // For market data channels (trades, l2Book, etc.), use "coin" parameter
        HyperliquidSubscribeMessage.Subscription subscription;
        if ("userFills".equals(type) || "userEvents".equals(type) || "orderUpdates".equals(type) || "userFundings".equals(type) || "userNonFundingLedgerUpdates".equals(type) || "webData2".equals(type) || "activeAssetData".equals(type)) {
            subscription = new HyperliquidSubscribeMessage.Subscription(type, identifier, true);
        } else {
            subscription = new HyperliquidSubscribeMessage.Subscription(type, identifier);
        }

        HyperliquidSubscribeMessage message = new HyperliquidSubscribeMessage(SUBSCRIBE, subscription);

        return objectMapper.writeValueAsString(message);
    }

    public String getPingMessage() throws IOException {

        HyperliquidPingMessage message = new HyperliquidPingMessage(PING);

        return objectMapper.writeValueAsString(message);
    }

    @Override
    public String getUnsubscribeMessage(String channelName, Object... args) throws IOException {
        String[] parts = channelName.split("\\.");
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid channel name: " + channelName);
        }

        String type = parts[0];
        String identifier = parts[1]; // Can be coin or user address

        // For user-specific channels (userFills, userEvents), use "user" parameter
        // For market data channels (trades, l2Book, etc.), use "coin" parameter
        HyperliquidSubscribeMessage.Subscription subscription;
        if ("userFills".equals(type) || "userEvents".equals(type) || "orderUpdates".equals(type)) {
            subscription = new HyperliquidSubscribeMessage.Subscription(type, identifier, true);
        } else {
            subscription = new HyperliquidSubscribeMessage.Subscription(type, identifier);
        }

        HyperliquidSubscribeMessage message = new HyperliquidSubscribeMessage(UNSUBSCRIBE, subscription);

        return objectMapper.writeValueAsString(message);
    }


    @Override public void resubscribeChannels() throws IOException {
        LOG.debug("hyperliquid resubscribeChannels : called from {}", Thread.currentThread().getStackTrace()[2]);

        if (!isSocketOpen()) {
            connect();
        }

/*    if (xSpec.getApiKey() != null && !isLoggedIn){
      try {
        synchronousLogin(10);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }*/
        super.resubscribeChannels();
    }

    public void setChannelInactiveHandler(
            WebSocketClientHandler.WebSocketMessageHandler channelInactiveHandler) {
        this.channelInactiveHandler = channelInactiveHandler;
    }

    @Override
    protected WebSocketClientHandler getWebSocketClientHandler(
            WebSocketClientHandshaker handshake, WebSocketClientHandler.WebSocketMessageHandler handler) {
        LOG.info("Registering HyperliquidWebSocketClientHandler");
        return new HyperliquidWebSocketClientHandler(handshake, handler);
    }

    /**
     * Custom client handler in order to execute an external, user-provided handler on channel events.
     */
    class HyperliquidWebSocketClientHandler extends NettyWebSocketClientHandler {

        public HyperliquidWebSocketClientHandler(
                WebSocketClientHandshaker handshake, WebSocketMessageHandler handler) {
            super(handshake, handler);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            // FIX: Dispose ping subscription FIRST to immediately stop any ping attempts on closed socket
            // This prevents "WebSocket is not open!" spam during disconnect/reconnect
            pingPongDisconnectIfConnected();

            super.channelInactive(ctx);
            if (channelInactiveHandler != null) {
                channelInactiveHandler.onMessage("WebSocket Client disconnected!");
            }
        }
    }
}