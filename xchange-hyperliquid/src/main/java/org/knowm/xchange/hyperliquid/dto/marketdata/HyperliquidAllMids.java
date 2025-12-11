package org.knowm.xchange.hyperliquid.dto.marketdata;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * DTO for Hyperliquid allMids API response
 * Returns mid prices for all coins
 */
public class HyperliquidAllMids {
    private final Map<String, BigDecimal> dynamicProperties = new HashMap<>();
    

    public HyperliquidAllMids() {}

    /**
     * Captures all dynamic properties during deserialization
     * This handles any key-value pair in the JSON
     */
    @JsonAnySetter
    public void setDynamicProperty(String key, String value) {
        try {
            dynamicProperties.put(key, new BigDecimal(value));
        } catch (NumberFormatException e) {
            // Skip non-numeric values or handle as needed
        }
    }

    /**
     * Provides all dynamic properties during serialization
     * This ensures the object can be serialized back to JSON
     */
    @JsonAnyGetter
    public Map<String, BigDecimal> getDynamicProperties() {
        return dynamicProperties;
    }

    /**
     * Get value by exact key
     */
    public BigDecimal getValue(String key) {
        return dynamicProperties.get(key);
    }

    /**
     * Get all keys
     */
    public Set<String> getKeys() {
        return dynamicProperties.keySet();
    }

    /**
     * Get all data as map
     */
    public Map<String, BigDecimal> getAllData() {
        return new HashMap<>(dynamicProperties);
    }

    /**
     * Check if a key exists
     */
    public boolean hasKey(String key) {
        return dynamicProperties.containsKey(key);
    }

    /**
     * Get the number of dynamic properties
     */
    public int size() {
        return dynamicProperties.size();
    }

    /**
     * Check if empty
     */
    public boolean isEmpty() {
        return dynamicProperties.isEmpty();
    }

    @Override
    public String toString() {
        return "HyperliquidDynamicData{" +
                "properties=" + dynamicProperties +
                '}';
    }
}