package org.knowm.xchange.hyperliquid.dto.marketdata;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Hyperliquid exchange metadata
 * Response from /info endpoint with type="meta"
 */
public class HyperliquidMeta {

    @JsonProperty("universe")
    private List<AssetInfo> universe;

    public List<AssetInfo> getUniverse() {
        return universe;
    }

    public void setUniverse(List<AssetInfo> universe) {
        this.universe = universe;
    }

    /**
     * Asset information within the metadata
     */
    public static class AssetInfo {
        @JsonProperty("name")
        private String name;

        @JsonProperty("szDecimals")
        private int szDecimals;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getSzDecimals() {
            return szDecimals;
        }

        public void setSzDecimals(int szDecimals) {
            this.szDecimals = szDecimals;
        }
    }
}
