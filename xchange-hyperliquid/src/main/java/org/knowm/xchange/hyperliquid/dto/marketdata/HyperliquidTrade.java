package org.knowm.xchange.hyperliquid.dto.marketdata;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.knowm.xchange.hyperliquid.dto.trade.Side;

import java.math.BigDecimal;

public class HyperliquidTrade {
    
    @JsonProperty("coin")
    private String coin;
    
    @JsonProperty("side")
    private Side side;
    
    @JsonProperty("px")
    private BigDecimal price;
    
    @JsonProperty("sz")
    private BigDecimal size;
    
    @JsonProperty("time")
    private Long time;
    @JsonProperty("tid")
    private Long tid;
    @JsonProperty("hash")
    private String hash;
    
    public HyperliquidTrade() {}
    
    public String getCoin() {
        return coin;
    }
    
    public void setCoin(String coin) {
        this.coin = coin;
    }
    
    public Side getSide() {
        return side;
    }
    
    public void setSide(Side side) {
        this.side = side;
    }
    public void setTid(Long tid) {
        this.tid = tid;
    }
    public Long getTid() {
        return tid;
    }
    public BigDecimal getPrice() {
        return price;
    }
    
    public void setPrice(BigDecimal price) {
        this.price = price;
    }
    
    public BigDecimal getSize() {
        return size;
    }
    
    public void setSize(BigDecimal size) {
        this.size = size;
    }
    
    public Long getTime() {
        return time;
    }
    
    public void setTime(Long time) {
        this.time = time;
    }
    
    public String getHash() {
        return hash;
    }
    
    public void setHash(String hash) {
        this.hash = hash;
    }
}