package org.knowm.xchange.hyperliquid.dto.account;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * DTO representing an asset position wrapper from Hyperliquid clearinghouseState.
 * Contains the position type and the actual position data.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HyperliquidAssetPosition {

    private final String type;
    private final HyperliquidPositionData position;

    public HyperliquidAssetPosition(
            @JsonProperty("type") String type,
            @JsonProperty("position") HyperliquidPositionData position) {
        this.type = type;
        this.position = position;
    }

    public String getType() {
        return type;
    }

    public HyperliquidPositionData getPosition() {
        return position;
    }

    @Override
    public String toString() {
        return "HyperliquidAssetPosition{" +
                "type='" + type + '\'' +
                ", position=" + position +
                '}';
    }
}
