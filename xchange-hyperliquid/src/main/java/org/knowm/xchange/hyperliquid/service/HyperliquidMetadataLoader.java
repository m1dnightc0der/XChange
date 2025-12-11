package org.knowm.xchange.hyperliquid.service;

import org.knowm.xchange.hyperliquid.HyperliquidInfo;
import org.knowm.xchange.hyperliquid.dto.marketdata.HyperliquidMeta;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Shared metadata loader for Hyperliquid exchange
 *
 * Loads exchange metadata (asset universe) and builds lookup mappings
 * for converting coin names to asset indices, which are required by the
 * Hyperliquid API for order placement and cancellation.
 *
 * Matches Python SDK's __init__ method that fetches meta and builds
 * coin_to_asset/name_to_coin mappings.
 * See: hyperliquid/info.py lines 71-76
 *
 * This class is thread-safe and uses lazy loading with double-checked locking.
 */
public class HyperliquidMetadataLoader {

    private final HyperliquidInfo hyperliquidInfo;

    // Cache for coin name to asset index mapping
    // Matches Python SDK's coin_to_asset and name_to_coin dictionaries
    private final Map<String, Integer> coinToAsset = new ConcurrentHashMap<>();
    private final Map<String, String> nameToCoin = new ConcurrentHashMap<>();
    private volatile boolean metadataLoaded = false;

    /**
     * Constructor
     *
     * @param hyperliquidInfo The HyperliquidInfo API interface for fetching metadata
     */
    public HyperliquidMetadataLoader(HyperliquidInfo hyperliquidInfo) {
        this.hyperliquidInfo = hyperliquidInfo;
    }

    /**
     * Load exchange metadata from the API
     *
     * Uses double-checked locking to ensure metadata is only loaded once,
     * even in multi-threaded environments.
     *
     * @throws IOException if the API request fails
     */
    public void loadMetadata() throws IOException {
        if (metadataLoaded) {
            return;
        }

        synchronized (this) {
            if (metadataLoaded) {
                return;
            }

            // Fetch metadata from /info endpoint with type="meta"
            Map<String, Object> requestBody = new LinkedHashMap<>();
            requestBody.put("type", "meta");
            requestBody.put("dex", ""); // Default perp dex (empty string represents original dex)

            HyperliquidMeta meta = hyperliquidInfo.getMeta("application/json", requestBody);

            // Build mappings: enumerate through universe and map names to indices
            // Matches Python: for asset, asset_info in enumerate(meta["universe"])
            List<HyperliquidMeta.AssetInfo> universe = meta.getUniverse();
            for (int asset = 0; asset < universe.size(); asset++) {
                HyperliquidMeta.AssetInfo assetInfo = universe.get(asset);
                String name = assetInfo.getName();

                // Matches Python lines 74-75
                coinToAsset.put(name, asset);
                nameToCoin.put(name, name);
            }

            metadataLoaded = true;
        }
    }

    /**
     * Convert coin name to asset index
     *
     * Matches Python SDK's name_to_asset method
     * See: hyperliquid/info.py lines 781-782
     *
     * @param name The coin/asset name (e.g., "BTC", "ETH", "SOL")
     * @return The asset index for API requests
     * @throws IOException if metadata cannot be loaded
     * @throws IllegalArgumentException if the coin name is unknown
     */
    public int getAssetIndex(String name) throws IOException {
        // Lazy load metadata on first use
        if (!metadataLoaded) {
            loadMetadata();
        }

        // Two-step lookup matching Python: self.coin_to_asset[self.name_to_coin[name]]
        String coin = nameToCoin.get(name);
        if (coin == null) {
            throw new IllegalArgumentException("Unknown coin: " + name);
        }

        Integer assetIndex = coinToAsset.get(coin);
        if (assetIndex == null) {
            throw new IllegalArgumentException("No asset index for coin: " + coin);
        }

        return assetIndex;
    }

    /**
     * Check if metadata has been loaded
     *
     * @return true if metadata has been loaded, false otherwise
     */
    public boolean isMetadataLoaded() {
        return metadataLoaded;
    }

    /**
     * Get all coin names that have been loaded
     *
     * @return Map of coin names to asset indices (read-only view)
     */
    public Map<String, Integer> getCoinToAssetMap() {
        return new LinkedHashMap<>(coinToAsset);
    }
}
