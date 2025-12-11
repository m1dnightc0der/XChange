package info.bitrich.xchangestream.hyperliquid.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HyperliquidPingMessage {

    @JsonProperty("method")
    private String method;

    public HyperliquidPingMessage() {}

    public HyperliquidPingMessage(String method) {
        this.method = method;

    }
    
    public String getMethod() {
        return method;
    }
    
    public void setMethod(String method) {
        this.method = method;
    }
    

}