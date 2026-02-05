package org.knowm.xchange.hyperliquid.dto.account;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * DTO representing cumulative funding data for a Hyperliquid position.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HyperliquidCumFunding {

    private final String allTime;
    private final String sinceOpen;
    private final String sinceChange;

    public HyperliquidCumFunding(
            @JsonProperty("allTime") String allTime,
            @JsonProperty("sinceOpen") String sinceOpen,
            @JsonProperty("sinceChange") String sinceChange) {
        this.allTime = allTime;
        this.sinceOpen = sinceOpen;
        this.sinceChange = sinceChange;
    }

    public String getAllTime() {
        return allTime;
    }

    public String getSinceOpen() {
        return sinceOpen;
    }

    public String getSinceChange() {
        return sinceChange;
    }

    @Override
    public String toString() {
        return "HyperliquidCumFunding{" +
                "allTime='" + allTime + '\'' +
                ", sinceOpen='" + sinceOpen + '\'' +
                ", sinceChange='" + sinceChange + '\'' +
                '}';
    }
}
