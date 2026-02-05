package org.knowm.xchange.hyperliquid.dto.account;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * DTO representing leverage settings for a Hyperliquid position.
 * For cross margin, rawUsd may be null.
 * For isolated margin, rawUsd contains the isolated margin amount.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HyperliquidLeverage {

    private final String type;  // "cross" or "isolated"
    private final Integer value;
    private final String rawUsd;  // Only present for isolated margin

    public HyperliquidLeverage(
            @JsonProperty("type") String type,
            @JsonProperty("value") Integer value,
            @JsonProperty("rawUsd") String rawUsd) {
        this.type = type;
        this.value = value;
        this.rawUsd = rawUsd;
    }

    public String getType() {
        return type;
    }

    public Integer getValue() {
        return value;
    }

    public String getRawUsd() {
        return rawUsd;
    }

    public boolean isCross() {
        return "cross".equalsIgnoreCase(type);
    }

    public boolean isIsolated() {
        return "isolated".equalsIgnoreCase(type);
    }

    @Override
    public String toString() {
        return "HyperliquidLeverage{" +
                "type='" + type + '\'' +
                ", value=" + value +
                ", rawUsd='" + rawUsd + '\'' +
                '}';
    }
}
