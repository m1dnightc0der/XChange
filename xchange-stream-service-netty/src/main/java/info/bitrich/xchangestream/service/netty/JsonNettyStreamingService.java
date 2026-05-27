package info.bitrich.xchangestream.service.netty;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class JsonNettyStreamingService extends NettyStreamingService<JsonNode> {
  private static final Logger LOG = LoggerFactory.getLogger(JsonNettyStreamingService.class);
  protected final ObjectMapper objectMapper = StreamingObjectMapperHelper.getObjectMapper();

  public JsonNettyStreamingService(String apiUrl) {
    super(apiUrl);
  }

  public JsonNettyStreamingService(String apiUrl, int maxFramePayloadLength) {
    super(apiUrl, maxFramePayloadLength);
  }

  public JsonNettyStreamingService(
      String apiUrl,
      int maxFramePayloadLength,
      Duration connectionTimeout,
      Duration retryDuration,
      int idleTimeoutSeconds) {
    super(apiUrl, maxFramePayloadLength, connectionTimeout, retryDuration, idleTimeoutSeconds);
  }

  public boolean processArrayMessageSeparately() {
    return true;
  }

  @Override
  public void messageHandler(String message) {
    LOG.debug("Received message: {}", message);
    JsonNode jsonNode;

    // Parse incoming message to JSON
    try {
      jsonNode = objectMapper.readTree(message);
    } catch (IOException e) {
      LOG.error("Error parsing incoming message to JSON: {}", message);
      return;
    }

    if (processArrayMessageSeparately() && jsonNode.isArray()) {
      // In case of array - handle every message separately.
      for (JsonNode node : jsonNode) {
        handleMessage(node);
      }
    } else {
      handleMessage(jsonNode);
    }
  }

  @Override
  protected Long latencyProbeExchangeTimestampMillis(JsonNode message) {
    JsonNode timestamp = findField(message, "timestamp");
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

  @Override
  protected String latencyProbeMessageHash(JsonNode message) {
    return Integer.toHexString(message.toString().hashCode());
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

  protected void sendObjectMessage(Object message) {
    try {
      sendMessage(objectMapper.writeValueAsString(message));
    } catch (JsonProcessingException e) {
      LOG.error("Error creating json message: {}", e.getMessage());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
