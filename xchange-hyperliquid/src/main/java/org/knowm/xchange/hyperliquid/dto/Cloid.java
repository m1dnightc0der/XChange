package org.knowm.xchange.hyperliquid.dto;

/**
 * Client Order ID (Cloid) for Hyperliquid orders
 *
 * Represents a client-assigned order identifier that must be a 16-byte hexadecimal string.
 * Matches the Python SDK's Cloid class implementation.
 *
 * Format requirements:
 * - Must start with "0x" prefix
 * - Must be exactly 16 bytes (32 hex characters after the prefix)
 * - Total length: 34 characters ("0x" + 32 hex chars)
 *
 * See: hyperliquid-python-sdk/hyperliquid/utils/types.py lines 192-219
 */
public class Cloid {
    private final String rawCloid;

    /**
     * Private constructor - use factory methods to create instances
     *
     * @param rawCloid The raw hex string representation
     */
    private Cloid(String rawCloid) {
        this.rawCloid = rawCloid;
        validate();
    }

    /**
     * Validate the cloid format
     *
     * @throws IllegalArgumentException if validation fails
     */
    private void validate() {
        if (rawCloid == null) {
            throw new IllegalArgumentException("cloid cannot be null");
        }

        if (!rawCloid.startsWith("0x")) {
            throw new IllegalArgumentException("cloid is not a hex string (must start with 0x)");
        }

        String hexPart = rawCloid.substring(2);
        if (hexPart.length() != 32) {
            throw new IllegalArgumentException(
                "cloid is not 16 bytes (must be 32 hex characters after 0x, got " + hexPart.length() + ")"
            );
        }

        // Validate that it's valid hex
        try {
            new java.math.BigInteger(hexPart, 16);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("cloid contains invalid hex characters: " + hexPart);
        }
    }

    /**
     * Create a Cloid from an integer value
     * The integer will be formatted as a 16-byte hex string with 0x prefix
     *
     * @param cloid The integer value
     * @return A new Cloid instance
     */
    public static Cloid fromInt(long cloid) {
        // Format as 16-byte (32 hex char) hex string with 0x prefix
        // %034x means: 0-padded, 34 total chars (including 0x), hex
        String hexString = String.format("0x%032x", cloid);
        return new Cloid(hexString);
    }

    /**
     * Create a Cloid from a hex string
     *
     * @param cloid The hex string (must be "0x" + 32 hex chars)
     * @return A new Cloid instance
     * @throws IllegalArgumentException if the format is invalid
     */
    public static Cloid fromStr(String cloid) {
        return new Cloid(cloid);
    }

    /**
     * Convert a full cloid hex string to its minimal representation (without leading zeros)
     * Static utility method that doesn't require creating a Cloid instance
     *
     * @param cloidStr The full cloid string (e.g., "0x0000000000000000000000000077dd55")
     * @return Minimal hex string without leading zeros and without 0x prefix (e.g., "77dd55"), or null if input is null or empty
     * @throws IllegalArgumentException if the input is not a valid hex string
     */
    public static String toMinimalHexString(String cloidStr) {
        if (cloidStr == null || cloidStr.isEmpty()) {
            return null;
        }

        // Remove the "0x" prefix if present
        String hexValue = cloidStr.startsWith("0x") ? cloidStr.substring(2) : cloidStr;

        // Validate it's valid hex
        try {
            java.math.BigInteger bigInt = new java.math.BigInteger(hexValue, 16);
            // Return minimal hex representation (without 0x prefix)
            return bigInt.toString(16);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid hex string: " + cloidStr, e);
        }
    }

    /**
     * Get the raw hex string representation
     *
     * @return The raw cloid string (e.g., "0x00000000000000000000000000000001")
     */
    public String toRaw() {
        return rawCloid;
    }

    /**
     * Convert the cloid back to its minimal hex representation (without leading zeros)
     *
     * @return Minimal hex string without leading zeros (e.g., "77dd55" from "0x0000000000000000000000000077dd55")
     */
    public String toMinimalHex() {
        // Remove the "0x" prefix and strip leading zeros
        String hexValue = rawCloid.substring(2);

        // Convert to BigInteger and back to hex to remove leading zeros
        java.math.BigInteger bigInt = new java.math.BigInteger(hexValue, 16);

        // Return minimal hex representation (without 0x prefix)
        return bigInt.toString(16);
    }

    /**
     * Convert the cloid back to its minimal hex representation with 0x prefix
     *
     * @return Minimal hex string with 0x prefix (e.g., "0x77dd55" from "0x0000000000000000000000000077dd55")
     */
    public String toMinimalHexWith0x() {
        return "0x" + toMinimalHex();
    }

    @Override
    public String toString() {
        return rawCloid;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Cloid other = (Cloid) obj;
        return rawCloid.equals(other.rawCloid);
    }

    @Override
    public int hashCode() {
        return rawCloid.hashCode();
    }
}
