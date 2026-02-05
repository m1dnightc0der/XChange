package org.knowm.xchange.hyperliquid.dto.account;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * DTO representing the Hyperliquid clearinghouseState response.
 * This contains the user's perpetuals account summary including positions,
 * margin summaries, and withdrawable amounts.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HyperliquidClearinghouseState {

    private final HyperliquidMarginSummary marginSummary;
    private final HyperliquidMarginSummary crossMarginSummary;
    private final String crossMaintenanceMarginUsed;
    private final String withdrawable;
    private final List<HyperliquidAssetPosition> assetPositions;
    private final Long time;

    public HyperliquidClearinghouseState(
            @JsonProperty("marginSummary") HyperliquidMarginSummary marginSummary,
            @JsonProperty("crossMarginSummary") HyperliquidMarginSummary crossMarginSummary,
            @JsonProperty("crossMaintenanceMarginUsed") String crossMaintenanceMarginUsed,
            @JsonProperty("withdrawable") String withdrawable,
            @JsonProperty("assetPositions") List<HyperliquidAssetPosition> assetPositions,
            @JsonProperty("time") Long time) {
        this.marginSummary = marginSummary;
        this.crossMarginSummary = crossMarginSummary;
        this.crossMaintenanceMarginUsed = crossMaintenanceMarginUsed;
        this.withdrawable = withdrawable;
        this.assetPositions = assetPositions;
        this.time = time;
    }

    public HyperliquidMarginSummary getMarginSummary() {
        return marginSummary;
    }

    public HyperliquidMarginSummary getCrossMarginSummary() {
        return crossMarginSummary;
    }

    public String getCrossMaintenanceMarginUsed() {
        return crossMaintenanceMarginUsed;
    }

    public String getWithdrawable() {
        return withdrawable;
    }

    public List<HyperliquidAssetPosition> getAssetPositions() {
        return assetPositions;
    }

    public Long getTime() {
        return time;
    }

    @Override
    public String toString() {
        return "HyperliquidClearinghouseState{" +
                "marginSummary=" + marginSummary +
                ", crossMarginSummary=" + crossMarginSummary +
                ", crossMaintenanceMarginUsed='" + crossMaintenanceMarginUsed + '\'' +
                ", withdrawable='" + withdrawable + '\'' +
                ", assetPositions=" + assetPositions +
                ", time=" + time +
                '}';
    }
}
