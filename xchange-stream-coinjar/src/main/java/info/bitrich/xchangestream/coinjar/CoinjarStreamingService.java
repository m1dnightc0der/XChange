package info.bitrich.xchangestream.coinjar;

import com.fasterxml.jackson.databind.JsonNode;
import info.bitrich.xchangestream.coinjar.dto.CoinjarHeartbeat;
import info.bitrich.xchangestream.coinjar.dto.CoinjarWebSocketSubscribeMessage;
import info.bitrich.xchangestream.coinjar.dto.CoinjarWebSocketUnsubscribeMessage;
import info.bitrich.xchangestream.service.netty.JsonNettyStreamingService;
import info.bitrich.xchangestream.service.netty.WebSocketClientHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.Disposable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class CoinjarStreamingService extends JsonNettyStreamingService {

  private final AtomicInteger refCount = new AtomicInteger();

  private String apiKey;

  // FIX: Store heartbeat subscription so it can be disposed when channel closes
  private Disposable heartbeatSubscription;

  public CoinjarStreamingService(String apiUrl, String apiKey) {
    super(apiUrl);
    this.apiKey = apiKey;
    // FIX: Store subscription reference so it can be disposed later
    heartbeatSubscription = Observable.interval(30, TimeUnit.SECONDS)
        .subscribe(
            t -> {
              if (this.isSocketOpen()) {
                this.sendObjectMessage(new CoinjarHeartbeat(refCount.incrementAndGet()));
              }
            });
  }

  // FIX: Add disposal method for heartbeat subscription
  public void heartbeatDisconnectIfConnected() {
    if (heartbeatSubscription != null && !heartbeatSubscription.isDisposed()) {
      heartbeatSubscription.dispose();
    }
  }

  @Override
  protected String getChannelNameFromMessage(JsonNode message) {
    return message.get("topic").asText();
  }

  @Override
  public String getSubscribeMessage(String channelName, Object... args) throws IOException {
    return objectMapper.writeValueAsString(
        new CoinjarWebSocketSubscribeMessage(channelName, apiKey, refCount.incrementAndGet()));
  }

  @Override
  public String getUnsubscribeMessage(String channelName, Object... args) throws IOException {
    CoinjarWebSocketUnsubscribeMessage message = new CoinjarWebSocketUnsubscribeMessage();
    return objectMapper.writeValueAsString(message);
  }

  // FIX: Override to use custom handler that disposes heartbeat on disconnect
  @Override
  protected WebSocketClientHandler getWebSocketClientHandler(
      WebSocketClientHandshaker handshake, WebSocketClientHandler.WebSocketMessageHandler handler) {
    return new CoinjarWebSocketClientHandler(handshake, handler);
  }

  /**
   * Custom client handler to dispose heartbeat subscription on channel close
   */
  class CoinjarWebSocketClientHandler extends NettyWebSocketClientHandler {

    public CoinjarWebSocketClientHandler(
        WebSocketClientHandshaker handshake, WebSocketMessageHandler handler) {
      super(handshake, handler);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      // FIX: Dispose heartbeat subscription to prevent memory leak
      heartbeatDisconnectIfConnected();
      super.channelInactive(ctx);
    }
  }
}
