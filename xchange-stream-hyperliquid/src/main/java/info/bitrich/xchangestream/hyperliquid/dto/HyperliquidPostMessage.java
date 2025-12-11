package info.bitrich.xchangestream.hyperliquid.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/**
 * DTO for Hyperliquid WebSocket POST requests
 * Based on: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/post-requests
 *
 * Format:
 * {
 *   "method": "post",
 *   "id": <unique number>,
 *   "request": {
 *     "type": "action",
 *     "payload": { ... }
 *   }
 * }
 */
public class HyperliquidPostMessage {

    @JsonProperty("method")
    private String method;

    @JsonProperty("id")
    private Long id;

    @JsonProperty("request")
    private Request request;

    public HyperliquidPostMessage() {}

    public HyperliquidPostMessage(String method, Long id, Request request) {
        this.method = method;
        this.id = id;
        this.request = request;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Request getRequest() {
        return request;
    }

    public void setRequest(Request request) {
        this.request = request;
    }

    public static class Request {
        @JsonProperty("type")
        private String type;

        @JsonProperty("payload")
        private Map<String, Object> payload;

        public Request() {}

        public Request(String type, Map<String, Object> payload) {
            this.type = type;
            this.payload = payload;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Map<String, Object> getPayload() {
            return payload;
        }

        public void setPayload(Map<String, Object> payload) {
            this.payload = payload;
        }
    }
}
