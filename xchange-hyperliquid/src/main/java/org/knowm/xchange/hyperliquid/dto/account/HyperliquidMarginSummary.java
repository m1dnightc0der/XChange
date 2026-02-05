package org.knowm.xchange.hyperliquid.dto.account;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * DTO representing margin summary data from Hyperliquid.
 * Used for both marginSummary and crossMarginSummary fields.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HyperliquidMarginSummary {

    private final String accountValue;
    private final String totalNtlPos;
    private final String totalRawUsd;
    private final String totalMarginUsed;

    public HyperliquidMarginSummary(
            @JsonProperty("accountValue") String accountValue,
            @JsonProperty("totalNtlPos") String totalNtlPos,
            @JsonProperty("totalRawUsd") String totalRawUsd,
            @JsonProperty("totalMarginUsed") String totalMarginUsed) {
        this.accountValue = accountValue;
        this.totalNtlPos = totalNtlPos;
        this.totalRawUsd = totalRawUsd;
        this.totalMarginUsed = totalMarginUsed;
    }

    public String getAccountValue() {
        return accountValue;
    }

    public String getTotalNtlPos() {
        return totalNtlPos;
    }

    public String getTotalRawUsd() {
        return totalRawUsd;
    }

    public String getTotalMarginUsed() {
        return totalMarginUsed;
    }

    @Override
    public String toString() {
        return "HyperliquidMarginSummary{" +
                "accountValue='" + accountValue + '\'' +
                ", totalNtlPos='" + totalNtlPos + '\'' +
                ", totalRawUsd='" + totalRawUsd + '\'' +
                ", totalMarginUsed='" + totalMarginUsed + '\'' +
                '}';
    }
}
