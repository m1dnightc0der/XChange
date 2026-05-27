package info.bitrich.xchangestream.service.netty;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketHandshakeException;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(WebSocketClientHandler.class);
  public static final String LATENCY_PROBE_PROPERTY = "xchangestream.latencyProbe";
  private static final boolean LATENCY_PROBE = Boolean.getBoolean(LATENCY_PROBE_PROPERTY);
  private static final ObjectMapper PROBE_OBJECT_MAPPER = StreamingObjectMapperHelper.getObjectMapper();
  private final StringBuilder currentMessage = new StringBuilder();

  public interface WebSocketMessageHandler {
    public void onMessage(String message);
  }

  protected final WebSocketClientHandshaker handshaker;
  protected final WebSocketMessageHandler handler;
  private ChannelPromise handshakeFuture;

  public WebSocketClientHandler(
      WebSocketClientHandshaker handshaker, WebSocketMessageHandler handler) {
    this.handshaker = handshaker;
    this.handler = handler;
  }

  public ChannelFuture handshakeFuture() {
    return handshakeFuture;
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) {
    handshakeFuture = ctx.newPromise();
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    handshaker.handshake(ctx.channel());
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    LOG.info("WebSocket Client disconnected! {}", ctx.channel());
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
    Channel ch = ctx.channel();
    if (!handshaker.isHandshakeComplete()) {
      try {
        handshaker.finishHandshake(ch, (FullHttpResponse) msg);
        LOG.info("WebSocket Client connected! {}", ctx.channel());
        handshakeFuture.setSuccess();
      } catch (WebSocketHandshakeException e) {
        LOG.error("WebSocket Client failed to connect. {} {}", e.getMessage(), ctx.channel());
        handshakeFuture.setFailure(e);
      }
      return;
    }

    if (msg instanceof FullHttpResponse) {
      FullHttpResponse response = (FullHttpResponse) msg;
      throw new IllegalStateException(
          "Unexpected FullHttpResponse (getStatus="
              + response.status()
              + ", content="
              + response.content().toString(CharsetUtil.UTF_8)
              + ')');
    }

    WebSocketFrame frame = (WebSocketFrame) msg;
    if (frame instanceof TextWebSocketFrame) {
      dealWithTextFrame((TextWebSocketFrame) frame);
    } else if (frame instanceof ContinuationWebSocketFrame) {
      dealWithContinuation((ContinuationWebSocketFrame) frame);
    } else if (frame instanceof PingWebSocketFrame) {
      LOG.debug("WebSocket Client received ping");
      ch.writeAndFlush(new PongWebSocketFrame(frame.content().retain()));
    } else if (frame instanceof PongWebSocketFrame) {
      LOG.debug("WebSocket Client received pong");
    } else if (frame instanceof CloseWebSocketFrame) {
      LOG.info("WebSocket Client received closing! {}", ctx.channel());
      ch.close();
    }
  }

  private void dealWithTextFrame(TextWebSocketFrame frame) {
    if (frame.isFinalFragment()) {
      dispatchMessage(frame.text());
      return;
    }
    currentMessage.append(frame.text());
  }

  private void dealWithContinuation(ContinuationWebSocketFrame frame) {
    currentMessage.append(frame.text());
    if (frame.isFinalFragment()) {
      dispatchMessage(currentMessage.toString());
      currentMessage.setLength(0);
    }
  }

  private void dispatchMessage(String message) {
    long receiveMs = System.currentTimeMillis();
    if (LATENCY_PROBE) {
      logLatencyProbe(message, receiveMs);
    }
    handler.onMessage(message);
  }

  private void logLatencyProbe(String message, long receiveMs) {
    try {
      JsonNode jsonNode = PROBE_OBJECT_MAPPER.readTree(message);
      Long exchangeMs = extractTimestampMillis(jsonNode);
      String channel = extractTextField(jsonNode, "channel");
      LOG.info(
          "XCHANGE_STREAM_LATENCY_PROBE stage=websocket_receive channel={} exchange_ms={} receive_ms={} exchange_to_receive_ms={} message_hash={} message_length={}",
          channel,
          exchangeMs,
          receiveMs,
          exchangeMs == null ? null : receiveMs - exchangeMs,
          Integer.toHexString(message.hashCode()),
          message.length());
    } catch (Exception e) {
      LOG.info(
          "XCHANGE_STREAM_LATENCY_PROBE stage=websocket_receive channel=null exchange_ms=null receive_ms={} exchange_to_receive_ms=null message_hash={} message_length={} parse_error={}",
          receiveMs,
          Integer.toHexString(message.hashCode()),
          message.length(),
          e.toString());
    }
  }

  private static Long extractTimestampMillis(JsonNode node) {
    JsonNode timestamp = findField(node, "timestamp");
    if (timestamp == null || !timestamp.isNumber()) {
      return null;
    }
    long value = timestamp.asLong();
    if (value > 100_000_000_000_000_000L) {
      return value / 1_000_000L;
    }
    if (value > 100_000_000_000_000L) {
      return value / 1_000L;
    }
    if (value < 10_000_000_000L) {
      return value * 1_000L;
    }
    return value;
  }

  private static String extractTextField(JsonNode node, String fieldName) {
    JsonNode field = findField(node, fieldName);
    return field == null || field.isNull() ? null : field.asText();
  }

  private static JsonNode findField(JsonNode node, String fieldName) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isObject()) {
      JsonNode direct = node.get(fieldName);
      if (direct != null) {
        return direct;
      }
      java.util.Iterator<JsonNode> values = node.elements();
      while (values.hasNext()) {
        JsonNode found = findField(values.next(), fieldName);
        if (found != null) {
          return found;
        }
      }
    } else if (node.isArray()) {
      for (JsonNode child : node) {
        JsonNode found = findField(child, fieldName);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error(
        "WebSocket client {} encountered exception ({} - {}). Closing",
        ctx.channel(),
        cause.getClass().getSimpleName(),
        cause.getMessage(),
        cause);
    if (!handshakeFuture.isDone()) {
      handshakeFuture.setFailure(cause);
    }
    ctx.close();

    try {

      throw cause;
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }
}
