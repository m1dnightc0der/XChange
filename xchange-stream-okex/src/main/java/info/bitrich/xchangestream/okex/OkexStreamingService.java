package info.bitrich.xchangestream.okex;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import info.bitrich.xchangestream.okex.dto.OkexLoginMessage;
import info.bitrich.xchangestream.okex.dto.OkexSubscribeMessage;
import info.bitrich.xchangestream.okex.dto.OkexUnSubscribeMessage;
import info.bitrich.xchangestream.service.netty.JsonNettyStreamingService;
import info.bitrich.xchangestream.service.netty.NettyStreamingService;
import info.bitrich.xchangestream.service.netty.WebSocketClientHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.reactivex.rxjava3.core.*;
import io.reactivex.rxjava3.disposables.Disposable;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.exceptions.ExchangeException;
import org.knowm.xchange.exceptions.NotYetImplementedForExchangeException;
import org.knowm.xchange.okex.dto.OkexInstType;
import org.knowm.xchange.service.BaseParamsDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class OkexStreamingService extends JsonNettyStreamingService {

  private static final Logger LOG = LoggerFactory.getLogger(OkexStreamingService.class);
  protected final Map<String, Subscription> subscriptions = new ConcurrentHashMap<>();
  private static final String LOGIN_SIGN_METHOD = "GET";
  private static final String LOGIN_SIGN_REQUEST_PATH = "/users/self/verify";

  private static final String SUBSCRIBE = "subscribe";
  private static final String UNSUBSCRIBE = "unsubscribe";

  public static final String TRADES = "trades";
  public static final String ORDERBOOK = "books";
  public static final String ORDERBOOK5 = "books5";

  public static final String ORDERBOOK50 = "books50-l2-tbt";
  public static final String FUNDING_RATE = "funding-rate";
  public static final String TICKERS = "tickers";
  public static final String USERTRADES = "orders";
  public static final String USERFILLS = "fills";
  public boolean isLoggedIn = false;
  private final AtomicReference<CompletableFuture<Void>> loginFuture = new AtomicReference<>();
  private final Observable<Long> pingPongSrc = Observable.interval(15, 15, TimeUnit.SECONDS);

  private WebSocketClientHandler.WebSocketMessageHandler channelInactiveHandler = null;

  private Disposable pingPongSubscription;

  private ExchangeSpecification xSpec = null;

  public OkexStreamingService(String apiUrl, ExchangeSpecification exchangeSpecification) {
    super(apiUrl);
    this.xSpec = exchangeSpecification;
  }

  ExchangeSpecification getExchangeSpecification(){
    return this.xSpec;
  }
  @Override public Completable connect() {

    LOG.debug("connect : called from {}", Thread.currentThread().getStackTrace()[2]);
    isLoggedIn = false;
    loginFuture.set(null);  // Reset login future on new connection
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
                this.sendMessage("ping");
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




  @Override public void resubscribeChannels() throws IOException {
    LOG.debug("okx resubscribeChannels : called from {}", Thread.currentThread().getStackTrace()[2]);

    if (!isSocketOpen()) {
      connect();
    }
    if (xSpec.getApiKey() != null && !isLoggedIn) {
      LOG.debug("connect: logging in");

      try {
        login().get(10, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
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


  public boolean isPrivateStreamReady() {
    return isSocketOpen() && isLoggedIn;
  }

  public CompletableFuture<Void> login() throws JsonProcessingException, ExecutionException, Exception {
    LOG.debug("login : called from {}", Thread.currentThread().getStackTrace()[2]);

    // If already logged in, return completed future
    if (isLoggedIn) {
      return CompletableFuture.completedFuture(null);
    }

    // If login is already in progress, return existing future
    CompletableFuture<Void> existingFuture = loginFuture.get();
    if (existingFuture != null && !existingFuture.isDone()) {
      return existingFuture;
    }

    // Create new login future
    CompletableFuture<Void> newFuture = new CompletableFuture<>();
    if (!loginFuture.compareAndSet(existingFuture, newFuture)) {
      // Another thread beat us, return their future
      return loginFuture.get();
    }

    Mac mac;
    try {
      mac = Mac.getInstance(BaseParamsDigest.HMAC_SHA_256);
      final SecretKey secretKey =
          new SecretKeySpec(
              xSpec.getSecretKey().getBytes(StandardCharsets.UTF_8), BaseParamsDigest.HMAC_SHA_256);
      mac.init(secretKey);
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      newFuture.completeExceptionally(new ExchangeException("Invalid API secret", e));
      return newFuture;
    }
    String timestamp = String.valueOf(System.currentTimeMillis() / 1000);
    String toSign = timestamp + LOGIN_SIGN_METHOD + LOGIN_SIGN_REQUEST_PATH;
    String sign =
        Base64.getEncoder().encodeToString(mac.doFinal(toSign.getBytes(StandardCharsets.UTF_8)));

    OkexLoginMessage message = new OkexLoginMessage();
    String passphrase = xSpec.getExchangeSpecificParametersItem("passphrase").toString();
    OkexLoginMessage.LoginArg loginArg =
        new OkexLoginMessage.LoginArg(xSpec.getApiKey(), passphrase, timestamp, sign);
    message.getArgs().add(loginArg);
    this.sendMessage(objectMapper.writeValueAsString(message)).sync();

    return newFuture;
  }

  @Override
  public void messageHandler(String message) {
    LOG.debug("Received message: {}", message);
    JsonNode jsonNode;

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

    if (jsonNode.has("event") && jsonNode.get("event").textValue().equals("channel-conn-count-error")) {
      LOG.warn("OKX connection count limit exceeded, will reconnect after delay");

      // Propagate error to pending operations
      ExchangeException error = new ExchangeException("Connection count limit exceeded, reconnecting...");
      propagateErrorToSingles(error);

      // Set retry delay to respect OKX's 3 connections/second rate limit
      setRetryDelay(Duration.ofSeconds(1));

      // Disconnect to trigger auto-reconnect with the delay
      disconnect().subscribe(
          () -> LOG.info("Disconnected after connection count error, will auto-reconnect"),
          e -> LOG.error("Error disconnecting after connection count error: {}", e.getMessage())
      );
      return;
    }

    if (jsonNode.has("event") && jsonNode.get("event").textValue().equals("login")) {
      if (jsonNode.has("code") && jsonNode.get("code").textValue().equals("0")) {
        isLoggedIn = true;
        LOG.info("OKX private websocket login succeeded");

        // Complete the login future
        CompletableFuture<Void> future = loginFuture.get();
        if (future != null) {
          future.complete(null);
        }

        try {
          resubscribeChannels();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
    if (jsonNode.has("event") && jsonNode.get("event").textValue().equals("error")) {
      String errorCode = jsonNode.has("code") ? jsonNode.get("code").textValue() : "unknown";
      String errorMsg = jsonNode.has("msg") ? jsonNode.get("msg").textValue() : "Unknown error";

      // Handle authentication errors
      if (errorCode.equals("60011") || errorCode.equals("60031")) {
        isLoggedIn = false;
        LOG.warn("OKX private websocket authentication error: {} (code: {})", errorMsg, errorCode);

        // Complete any pending login future exceptionally
        CompletableFuture<Void> future = loginFuture.get();
        if (future != null && !future.isDone()) {
          future.completeExceptionally(new ExchangeException("Login failed: " + errorMsg + " (code: " + errorCode + ")"));
        }

        // Propagate error to all pending single emitters
        propagateErrorToSingles(new ExchangeException("Authentication error: " + errorMsg + " (code: " + errorCode + ")"));

        // Trigger re-login for subsequent requests
        try {
          loginFuture.set(null);  // Reset so login() creates a new future
          login();  // Attempt re-login
        } catch (Exception e) {
          LOG.warn("Failed to re-login after auth error: {}", e.getMessage());
        }
      } else {
        // For other errors, also propagate to singles
        propagateErrorToSingles(new ExchangeException("OKX error: " + errorMsg + " (code: " + errorCode + ")"));
      }
    }

    if (jsonNode.has("id") && jsonNode.has("op") && jsonNode.get("op").textValue().equals("order")) {

      ObservableEmitter<JsonNode> emitter = singles.get(jsonNode.get("id").asText());
      if (emitter == null) {
        LOG.debug("No emitter for single {}.", jsonNode.get("id"));
        return;
      }

      //emitter.onSuccess(jsonNode);

      emitter.onNext(jsonNode);
      singles.remove(jsonNode.get("id").asText());
      emitter.onComplete();

    } else {

/*



        ListIterator<LoginListener> iter = listeners.listIterator();
        while(iter.hasNext()){
          LoginListener listener = (LoginListener) iter.next();
          if(listener.getChannel().equals(getChannel())){
            listener.getLatch().countDown();
            if(listener.getLatch().getCount()==0) {
              iter.remove();
            }
            break;
          }
        }

        setLoggedInToFalse();

      }
      if (jsonNode.has("code") && jsonNode.get("code").textValue().equals("60031")) {
        isLoggedIn = false;
        ListIterator<LoginListener> iter = listeners.listIterator();
        while(iter.hasNext()){
          LoginListener listener = (LoginListener) iter.next();
          if(listener.getChannel().equals(getChannel())){
            listener.getLatch().countDown();
            if(listener.getLatch().getCount()==0) {
              iter.remove();
            }
            break;
          }
        }
      }
    }
    if (jsonNode.has("event") && jsonNode.get("event").textValue().equals("login")) {
      if (jsonNode.has("code") && jsonNode.get("code").textValue().equals("0")) {
        isLoggedIn = true;
        ListIterator<LoginListener> iter = listeners.listIterator();
        while(iter.hasNext()){
          LoginListener listener = (LoginListener) iter.next();
          if(listener.getChannel().equals(getChannel())){
            listener.getLatch().countDown();
            if(listener.getLatch().getCount()==0) {
              iter.remove();
            }
            break;
          }
        }
        try {
          resubscribeChannels();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }*/
      if (processArrayMessageSeparately() && jsonNode.isArray()) {
        // In case of array - handle every message separately.
        for (JsonNode node : jsonNode) {
          handleMessage(node);
        }
      } else {

        handleMessage(jsonNode);
      }
    }
  }

  /**
   * Propagate an error to all waiting single emitters.
   * This ensures that pending orders receive error feedback instead of timing out.
   */
  private void propagateErrorToSingles(Throwable error) {
    for (Map.Entry<String, ObservableEmitter<JsonNode>> entry : singles.entrySet()) {
      ObservableEmitter<JsonNode> emitter = entry.getValue();
      if (emitter != null && !emitter.isDisposed()) {
        try {
          emitter.onError(error);
        } catch (Exception e) {
          LOG.debug("Error propagating error to emitter {}: {}", entry.getKey(), e.getMessage());
        }
      }
    }
    singles.clear();
  }

  @Override
  protected String getChannelNameFromMessage(JsonNode message) {
    String channelName = "";
    if (message.has("arg")) {
      if (message.get("arg").has("channel") && message.get("arg").has("instId")) {
        channelName = message.get("arg").get("channel").asText() + message.get("arg").get("instId").asText();
      } else if (message.get("arg").has("channel")) {
        channelName = message.get("arg").get("channel").asText();

      }
    }
    return channelName;
  }

  @Override
  public String getSubscribeMessage(String channelName, Object... args) throws IOException {
    if (channelName.equals(USERTRADES) || channelName.equals(USERFILLS)) {
      try {
        login().get(10, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return objectMapper.writeValueAsString(
        new OkexSubscribeMessage(SUBSCRIBE, Collections.singletonList(getTopic(channelName))));
  }

  @Override
  public String getUnsubscribeMessage(String channelName, Object... args) throws IOException {
    OkexSubscribeMessage.SubscriptionTopic topic = getTopic(channelName);
    String msg = objectMapper.writeValueAsString(new OkexUnSubscribeMessage(UNSUBSCRIBE, Collections.singletonList(getTopic(channelName))));
    return msg;
  }

  private OkexSubscribeMessage.SubscriptionTopic getTopic(String channelName) {
    if (channelName.contains(ORDERBOOK50)) {
      return new OkexSubscribeMessage.SubscriptionTopic(ORDERBOOK50, null, null, channelName.replace(ORDERBOOK50, ""));
    } else if (channelName.contains(ORDERBOOK5)) {
      return new OkexSubscribeMessage.SubscriptionTopic(ORDERBOOK5, null, null, channelName.replace(ORDERBOOK5, ""));
    } else if (channelName.contains(ORDERBOOK)) {
      return new OkexSubscribeMessage.SubscriptionTopic(ORDERBOOK, null, null, channelName.replace(ORDERBOOK, ""));
    } else if (channelName.contains(TRADES)) {
      return new OkexSubscribeMessage.SubscriptionTopic(TRADES, null, null, channelName.replace(TRADES, ""));
    } else if (channelName.contains(TICKERS)) {
      return new OkexSubscribeMessage.SubscriptionTopic(TICKERS, null, null, channelName.replace(TICKERS, ""));
    } else if (channelName.contains(USERTRADES)) {
      return new OkexSubscribeMessage.SubscriptionTopic(USERTRADES, OkexInstType.ANY, null, channelName.replace(USERTRADES, ""));
    } else if (channelName.contains(FUNDING_RATE)) {
      return new OkexSubscribeMessage.SubscriptionTopic(FUNDING_RATE, null, null, channelName.replace(FUNDING_RATE, ""));
    } else {
      throw new NotYetImplementedForExchangeException(
          "ChannelName: " + channelName + " has not implemented yet on " + this.getClass().getSimpleName());
    }
  }

  @Override
  protected WebSocketClientHandler getWebSocketClientHandler(
      WebSocketClientHandshaker handshake, WebSocketClientHandler.WebSocketMessageHandler handler) {
    LOG.info("Registering OkxWebSocketClientHandler");
    return new OkxWebSocketClientHandler(handshake, handler);
  }

  public void setChannelInactiveHandler(
      WebSocketClientHandler.WebSocketMessageHandler channelInactiveHandler) {
    this.channelInactiveHandler = channelInactiveHandler;
  }

  /**
   * Custom client handler in order to execute an external, user-provided handler on channel events.
   */
  class OkxWebSocketClientHandler extends NettyWebSocketClientHandler {

    public OkxWebSocketClientHandler(
        WebSocketClientHandshaker handshake, WebSocketMessageHandler handler) {
      super(handshake, handler);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
      super.channelActive(ctx);
    }

    @Override public void channelInactive(ChannelHandlerContext ctx) {
      // FIX: Dispose ping subscription FIRST to immediately stop any ping attempts on closed socket
      // This prevents "WebSocket is not open!" spam during disconnect/reconnect
      pingPongDisconnectIfConnected();

      isLoggedIn = false;
      // Cancel any pending login future
      CompletableFuture<Void> future = loginFuture.get();
      if (future != null && !future.isDone()) {
        future.completeExceptionally(new ExchangeException("Connection closed"));
      }
      loginFuture.set(null);
      super.channelInactive(ctx);
      if (channelInactiveHandler != null) {
        channelInactiveHandler.onMessage("WebSocket Client disconnected!");
      }
    }
  }

  public void pingPongDisconnectIfConnected() {
    if (pingPongSubscription != null && !pingPongSubscription.isDisposed()) {
      pingPongSubscription.dispose();
    }
  }
}
