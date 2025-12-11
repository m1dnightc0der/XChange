package org.knowm.xchange.hyperliquid.dto.marketdata;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.util.List;

/**
 * DTO for Hyperliquid candleSnapshot API response
 * Returns candlestick/OHLCV data for a specific coin and interval
 */
public class HyperliquidCandleSnapshot {

    @JsonProperty("s")
    private String status;

    @JsonProperty("t")
    private List<Long> timestamps;

    @JsonProperty("o")
    private List<BigDecimal> opens;

    @JsonProperty("h")
    private List<BigDecimal> highs;

    @JsonProperty("l")
    private List<BigDecimal> lows;

    @JsonProperty("c")
    private List<BigDecimal> closes;

    @JsonProperty("v")
    private List<BigDecimal> volumes;

    public HyperliquidCandleSnapshot() {}

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public List<Long> getTimestamps() {
        return timestamps;
    }

    public void setTimestamps(List<Long> timestamps) {
        this.timestamps = timestamps;
    }

    public List<BigDecimal> getOpens() {
        return opens;
    }

    public void setOpens(List<BigDecimal> opens) {
        this.opens = opens;
    }

    public List<BigDecimal> getHighs() {
        return highs;
    }

    public void setHighs(List<BigDecimal> highs) {
        this.highs = highs;
    }

    public List<BigDecimal> getLows() {
        return lows;
    }

    public void setLows(List<BigDecimal> lows) {
        this.lows = lows;
    }

    public List<BigDecimal> getCloses() {
        return closes;
    }

    public void setCloses(List<BigDecimal> closes) {
        this.closes = closes;
    }

    public List<BigDecimal> getVolumes() {
        return volumes;
    }

    public void setVolumes(List<BigDecimal> volumes) {
        this.volumes = volumes;
    }

    /**
     * Helper class to represent individual candle data
     */
    public static class Candle {
        private final Long timestamp;
        private final BigDecimal open;
        private final BigDecimal high;
        private final BigDecimal low;
        private final BigDecimal close;
        private final BigDecimal volume;

        public Candle(Long timestamp, BigDecimal open, BigDecimal high, BigDecimal low, BigDecimal close, BigDecimal volume) {
            this.timestamp = timestamp;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
            this.volume = volume;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public BigDecimal getOpen() {
            return open;
        }

        public BigDecimal getHigh() {
            return high;
        }

        public BigDecimal getLow() {
            return low;
        }

        public BigDecimal getClose() {
            return close;
        }

        public BigDecimal getVolume() {
            return volume;
        }

        @Override
        public String toString() {
            return "Candle{" +
                    "timestamp=" + timestamp +
                    ", open=" + open +
                    ", high=" + high +
                    ", low=" + low +
                    ", close=" + close +
                    ", volume=" + volume +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "HyperliquidCandleSnapshot{" +
                "status='" + status + '\'' +
                ", timestamps=" + timestamps +
                ", opens=" + opens +
                ", highs=" + highs +
                ", lows=" + lows +
                ", closes=" + closes +
                ", volumes=" + volumes +
                '}';
    }
}