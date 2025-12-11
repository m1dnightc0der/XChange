package org.knowm.xchange.hyperliquid.dto.marketdata;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

public class HyperliquidL2Book {
    private final TreeMap<BigDecimal, BigDecimal> bids = new TreeMap<>((k1, k2) -> -k1.compareTo(k2));
    private final TreeMap<BigDecimal, BigDecimal> asks = new TreeMap<>();

    @JsonProperty("coin")
    private String coin;
    
    @JsonProperty("levels")
    public void setBidsAndAsks(List<List<Level>> levels)  {
        convertLevels(levels, this.bids,this.asks);
    }
    private List<List<Level>> levels;

    public Date getTimestamp() {
        return new Date(time);
    }
    @JsonProperty("time")
    private Long time;
    
    public HyperliquidL2Book() {}
    
    public String getCoin() {
        return coin;
    }
    
    public void setCoin(String coin) {
        this.coin = coin;
    }
    
    public List<List<Level>> getLevels() {
        return levels;
    }


    public TreeMap<BigDecimal, BigDecimal> getBids() {
        return bids;
    }
    public TreeMap<BigDecimal, BigDecimal> getAsks() {
        return asks;
    }
    public Long getTime() {
        return time;
    }
    
    public void setTime(Long time) {
        this.time = time;
    }
    
    public static class Level {

        @JsonProperty("px")
        private BigDecimal price;
        @JsonProperty("sz")
        private BigDecimal size;
        @JsonProperty("n")
        private BigDecimal orders;
        public Level() {
            super();
        }
        public Level(BigDecimal price, BigDecimal size, BigDecimal orders) {
            super();
            this.price = price;
            this.size = size;
            this.orders = orders;
        }
        
        public BigDecimal getPrice() {
            return price;
        }
        
        public BigDecimal getSize() {
            return size;
        }
        
        public BigDecimal getOrders() {
            return orders;
        }
    }

    private static void convertLevels(
            List<List<Level>> from, TreeMap<BigDecimal, BigDecimal> bids,  TreeMap<BigDecimal, BigDecimal> asks) {
        from.get(0).forEach(l ->  bids.put(l.price,l.size));
        from.get(1).forEach(l ->  asks.put(l.price,l.size));
    }
}