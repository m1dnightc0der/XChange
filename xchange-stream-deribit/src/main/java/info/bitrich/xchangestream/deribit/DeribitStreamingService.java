package info.bitrich.xchangestream.deribit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import info.bitrich.xchangestream.deribit.dto.DeribitLoginMessage;
import info.bitrich.xchangestream.deribit.dto.DeribitSubscribeMessage;
import info.bitrich.xchangestream.service.netty.JsonNettyStreamingService;
import info.bitrich.xchangestream.service.netty.WebSocketClientHandler;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.CompletableSource;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.reactivex.rxjava3.disposables.Disposable;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.exceptions.ExchangeException;

import org.knowm.xchange.exceptions.NotYetImplementedForExchangeException;
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
import java.util.*;
import java.util.concurrent.*;

public class DeribitStreamingService extends JsonNettyStreamingService {

  private static final Logger LOG = LoggerFactory.getLogger(DeribitStreamingService.class);
  private static final String LOGIN_SIGN_METHOD = "GET";
  private static final String LOGIN_SIGN_REQUEST_PATH = "/users/self/verify";

  private static final String SUBSCRIBE = "public/subscribe";
  private static final String UNSUBSCRIBE = "public/unsubscribe";

  public static final String TRADES = "trades";
  public static final String TICKER = "ticker";
  public static final String ORDERBOOK = "book";
  public static final String USERTRADES = "user.orders";
  public boolean isLoggedIn = false;
  private final Observable<Long> pingPongSrc = Observable.interval(15, 15, TimeUnit.SECONDS);

  private WebSocketClientHandler.WebSocketMessageHandler channelInactiveHandler = null;

  private Disposable pingPongSubscription;

  private final ExchangeSpecification xSpec;

  public DeribitStreamingService(String apiUrl, ExchangeSpecification exchangeSpecification) {
    super(apiUrl);
    this.xSpec = exchangeSpecification;
  }

  @Override public Completable connect() {

    LOG.debug("connect : called from {}", Thread.currentThread().getStackTrace()[2]);
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
    LOG.debug("deribit resubscribeChannels : called from {}", Thread.currentThread().getStackTrace()[2]);

    if (!isSocketOpen()) {
      connect();
    }
    if (xSpec.getApiKey() != null && !isLoggedIn) {
      LOG.debug("connect: loging in");

      try {
        login();
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

  public void login() throws JsonProcessingException, ExecutionException, Exception {
    LOG.debug("login : called from {}", Thread.currentThread().getStackTrace()[2]);
    Mac mac;
    try {
      mac = Mac.getInstance(BaseParamsDigest.HMAC_SHA_256);
      final SecretKey secretKey =
          new SecretKeySpec(
              xSpec.getSecretKey().getBytes(StandardCharsets.UTF_8), BaseParamsDigest.HMAC_SHA_256);
      mac.init(secretKey);
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      throw new ExchangeException("Invalid API secret", e);
    }
    String timestamp = String.valueOf(System.currentTimeMillis() / 1000);
    String toSign = timestamp + LOGIN_SIGN_METHOD + LOGIN_SIGN_REQUEST_PATH;
    String sign =
        Base64.getEncoder().encodeToString(mac.doFinal(toSign.getBytes(StandardCharsets.UTF_8)));

    DeribitLoginMessage message = new DeribitLoginMessage();
        DeribitLoginMessage.LoginArg loginArg =
        new DeribitLoginMessage.LoginArg(xSpec.getApiKey(),xSpec.getSecretKey(),"client_credentials", timestamp, sign);
    message.setParams((loginArg));
    this.sendMessage(objectMapper.writeValueAsString(message)).sync();

  }

  @Override
  public void messageHandler(String message)  {
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


      else if(uri.getPath().toLowerCase().contains("private")) {
        throw new RuntimeException(e);
      } else {
        LOG.error("Error parsing incoming message to JSON: {}", message);
        return;
      }
    }

    if (jsonNode!=null && jsonNode.has("event") && jsonNode.get("event").textValue().equals("channel-conn-count-error")) {
      throw new ExchangeException("Connection Count Exceeded");
    }

    if (jsonNode!=null &&  jsonNode.has("result") && jsonNode.get("result").has("access_token")) {
            isLoggedIn = true;
            LOG.info("Deribit private websocket login succeeded");


    }
    if (jsonNode!=null &&  jsonNode.has("error") && jsonNode.get("error").has("code")) {
     // if (jsonNode.get("error").get("code").textValue().equals("13004")) {
        isLoggedIn = false;
        LOG.warn("Deribit private websocket authentication error: {}", jsonNode.get("error"));
      //}
   }
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

    if (jsonNode!=null && jsonNode.has("id")) {

      ObservableEmitter<JsonNode> emitter = singles.get(jsonNode.get("id").asText());
      if (emitter == null) {
        LOG.debug("No emitter for single {}.", jsonNode.get("id"));
        return;
      }

      //emitter.onSuccess(jsonNode);

      emitter.onNext(jsonNode);
      singles.remove(jsonNode.get("id").asText());
      emitter.onComplete();

    }else {
      if (jsonNode!=null &&processArrayMessageSeparately() && jsonNode.isArray()) {
        // In case of array - handle every message separately.
        for (JsonNode node : jsonNode) {
          handleMessage(node);
        }
      } else if (jsonNode!=null) {

        handleMessage(jsonNode);
      }
    }
  }

  @Override
  protected String getChannelNameFromMessage(JsonNode message) {
    String channelName = "";
    if (message.has("params")) {
      if (message.get("params").has("channel")) {
        channelName = message.get("params").get("channel").asText();
      }
    }
    return channelName;
  }

  @Override
  public String getSubscribeMessage(String channelName, Object... args) throws IOException {
    return objectMapper.writeValueAsString(
        new DeribitSubscribeMessage(SUBSCRIBE, (getTopic(channelName))));
  }

  @Override
  public String getUnsubscribeMessage(String channelName, Object... args) throws IOException {
    String msg = objectMapper.writeValueAsString(new DeribitSubscribeMessage(UNSUBSCRIBE,(getTopic(channelName))));
    return msg;
  }

  private DeribitSubscribeMessage.SubscriptionTopic getTopic(String channelName) {
     if (channelName.contains(ORDERBOOK)) {
      return new DeribitSubscribeMessage.SubscriptionTopic( Arrays.asList(channelName ));
    } else if (channelName.contains(TRADES)) {
      return new DeribitSubscribeMessage.SubscriptionTopic( Arrays.asList(channelName));
     } else if (channelName.contains(TICKER)) {
       return new DeribitSubscribeMessage.SubscriptionTopic( Arrays.asList(channelName));
    } else if (channelName.contains(USERTRADES)) {
      return new DeribitSubscribeMessage.SubscriptionTopic(Arrays.asList(channelName));
    } else {
      throw new NotYetImplementedForExchangeException(
          "ChannelName: " + channelName + " has not implemented yet on " + this.getClass().getSimpleName());
    }
  }

  @Override
  protected WebSocketClientHandler getWebSocketClientHandler(
      WebSocketClientHandshaker handshake, WebSocketClientHandler.WebSocketMessageHandler handler) {
    LOG.info("Registering DeribitWebSocketClientHandler");
    return new DeribitWebSocketClientHandler(handshake, handler);
  }

  public void setChannelInactiveHandler(
      WebSocketClientHandler.WebSocketMessageHandler channelInactiveHandler) {
    this.channelInactiveHandler = channelInactiveHandler;
  }

  /**
   * Custom client handler in order to execute an external, user-provided handler on channel events.
   */
  class DeribitWebSocketClientHandler extends NettyWebSocketClientHandler {

    public DeribitWebSocketClientHandler(
        WebSocketClientHandshaker handshake, WebSocketMessageHandler handler) {
      super(handshake, handler);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
      super.channelActive(ctx);
    }

    @Override public void channelInactive(ChannelHandlerContext ctx) {
      isLoggedIn=false;
      pingPongDisconnectIfConnected();
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
