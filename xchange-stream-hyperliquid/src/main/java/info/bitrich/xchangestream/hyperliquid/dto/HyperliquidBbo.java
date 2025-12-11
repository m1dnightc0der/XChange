package info.bitrich.xchangestream.hyperliquid.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.knowm.xchange.hyperliquid.dto.marketdata.HyperliquidL2Book;

import java.math.BigDecimal;
import java.util.List;
import java.util.TreeMap;

public class HyperliquidBbo {
    private final TreeMap<BigDecimal, BigDecimal> bids = new TreeMap<>((k1, k2) -> -k1.compareTo(k2));
    private final TreeMap<BigDecimal, BigDecimal> asks = new TreeMap<>();
    @JsonProperty("coin")
    private String coin;

    @JsonProperty("bbo")
    public void setBidsAndAsks(List<HyperliquidBbo.Bbo> Bbos)  {
        convertBBO(Bbos, this.bids,this.asks);
    }
    public TreeMap<BigDecimal, BigDecimal> getBids() {
        return bids;
    }
    public TreeMap<BigDecimal, BigDecimal> getAsks() {
        return asks;
    }
    public static class Bbo {

        @JsonProperty("px")
        private BigDecimal price;
        @JsonProperty("sz")
        private BigDecimal size;
        @JsonProperty("n")
        private BigDecimal orders;
        public Bbo() {
            super();
        }
        public Bbo(BigDecimal price, BigDecimal size, BigDecimal orders) {
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
    @JsonProperty("time")
    private Long time;
    
    public HyperliquidBbo() {}
    
    public String getCoin() {
        return coin;
    }
    
    public void setCoin(String coin) {
        this.coin = coin;
    }

    
    public Long getTime() {
        return time;
    }
    
    public void setTime(Long time) {
        this.time = time;
    }

    private static void convertBBO(
            List<HyperliquidBbo.Bbo> from, TreeMap<BigDecimal, BigDecimal> bids, TreeMap<BigDecimal, BigDecimal> asks) {
        bids.put(from.get(0).price,from.get(0).size);
        asks.put(from.get(1).price,from.get(1).size);

    }
}