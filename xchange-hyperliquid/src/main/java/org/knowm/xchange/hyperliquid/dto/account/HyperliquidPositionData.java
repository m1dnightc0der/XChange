package org.knowm.xchange.hyperliquid.dto.account;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * DTO representing the actual position data from Hyperliquid.
 * Contains coin, size, leverage, entry price, PnL, and other position details.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HyperliquidPositionData {

    private final String coin;
    private final String szi;  // Size (negative for short positions)
    private final HyperliquidLeverage leverage;
    private final String entryPx;
    private final String positionValue;
    private final String unrealizedPnl;
    private final String returnOnEquity;
    private final String liquidationPx;
    private final String marginUsed;
    private final Integer maxLeverage;
    private final HyperliquidCumFunding cumFunding;

    public HyperliquidPositionData(
            @JsonProperty("coin") String coin,
            @JsonProperty("szi") String szi,
            @JsonProperty("leverage") HyperliquidLeverage leverage,
            @JsonProperty("entryPx") String entryPx,
            @JsonProperty("positionValue") String positionValue,
            @JsonProperty("unrealizedPnl") String unrealizedPnl,
            @JsonProperty("returnOnEquity") String returnOnEquity,
            @JsonProperty("liquidationPx") String liquidationPx,
            @JsonProperty("marginUsed") String marginUsed,
            @JsonProperty("maxLeverage") Integer maxLeverage,
            @JsonProperty("cumFunding") HyperliquidCumFunding cumFunding) {
        this.coin = coin;
        this.szi = szi;
        this.leverage = leverage;
        this.entryPx = entryPx;
        this.positionValue = positionValue;
        this.unrealizedPnl = unrealizedPnl;
        this.returnOnEquity = returnOnEquity;
        this.liquidationPx = liquidationPx;
        this.marginUsed = marginUsed;
        this.maxLeverage = maxLeverage;
        this.cumFunding = cumFunding;
    }

    public String getCoin() {
        return coin;
    }

    public String getSzi() {
        return szi;
    }

    public HyperliquidLeverage getLeverage() {
        return leverage;
    }

    public String getEntryPx() {
        return entryPx;
    }

    public String getPositionValue() {
        return positionValue;
    }

    public String getUnrealizedPnl() {
        return unrealizedPnl;
    }

    public String getReturnOnEquity() {
        return returnOnEquity;
    }

    public String getLiquidationPx() {
        return liquidationPx;
    }

    public String getMarginUsed() {
        return marginUsed;
    }

    public Integer getMaxLeverage() {
        return maxLeverage;
    }

    public HyperliquidCumFunding getCumFunding() {
        return cumFunding;
    }

    @Override
    public String toString() {
        return "HyperliquidPositionData{" +
                "coin='" + coin + '\'' +
                ", szi='" + szi + '\'' +
                ", leverage=" + leverage +
                ", entryPx='" + entryPx + '\'' +
                ", positionValue='" + positionValue + '\'' +
                ", unrealizedPnl='" + unrealizedPnl + '\'' +
                ", returnOnEquity='" + returnOnEquity + '\'' +
                ", liquidationPx='" + liquidationPx + '\'' +
                ", marginUsed='" + marginUsed + '\'' +
                ", maxLeverage=" + maxLeverage +
                ", cumFunding=" + cumFunding +
                '}';
    }
}
