package org.knowm.xchange.hyperliquid.dto;

import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test hex string padding for Cloid generation
 */
public class CloidPaddingTest {

    @Test
    public void testPaddingShortHexString() {
        // Test the same logic used in HyperliquidStreamingTradeService
        String userRef = "77dd55";

        // Handle hex string input - pad to 32 hex characters
        String hexValue = userRef.startsWith("0x") ? userRef.substring(2) : userRef;
        String paddedHex = String.format("%032x", new java.math.BigInteger(hexValue, 16));
        Cloid cloid = Cloid.fromStr("0x" + paddedHex);

        // Verify the result - actual output has 30 hex chars (77dd55 = 6 chars, padded to 30)
        assertThat(cloid.toRaw()).isEqualTo("0x0000000000000000000000000077dd55");
        assertThat(cloid.toString()).isEqualTo("0x0000000000000000000000000077dd55");

        // Verify it's a valid 16-byte cloid (32 hex chars after 0x)
        String raw = cloid.toRaw();
        assertThat(raw.substring(2)).hasSize(32);
    }

    @Test
    public void testPaddingWithPrefix() {
        // Test with 0x prefix
        String userRef = "0x77dd55";

        String hexValue = userRef.startsWith("0x") ? userRef.substring(2) : userRef;
        String paddedHex = String.format("%032x", new java.math.BigInteger(hexValue, 16));
        Cloid cloid = Cloid.fromStr("0x" + paddedHex);

        assertThat(cloid.toRaw()).isEqualTo("0x0000000000000000000000000077dd55");
        assertThat(cloid.toRaw().substring(2)).hasSize(32);
    }

    @Test
    public void testPaddingSingleDigit() {
        // Test with single digit
        String userRef = "1";

        String hexValue = userRef.startsWith("0x") ? userRef.substring(2) : userRef;
        String paddedHex = String.format("%032x", new java.math.BigInteger(hexValue, 16));
        Cloid cloid = Cloid.fromStr("0x" + paddedHex);

        assertThat(cloid.toRaw()).isEqualTo("0x00000000000000000000000000000001");
        assertThat(cloid.toRaw().substring(2)).hasSize(32);
    }

    @Test
    public void testPaddingLongHexString() {
        // Test with longer hex string (but still less than 32 chars)
        String userRef = "deadbeef";

        String hexValue = userRef.startsWith("0x") ? userRef.substring(2) : userRef;
        String paddedHex = String.format("%032x", new java.math.BigInteger(hexValue, 16));
        Cloid cloid = Cloid.fromStr("0x" + paddedHex);

        assertThat(cloid.toRaw()).isEqualTo("0x000000000000000000000000deadbeef");
        assertThat(cloid.toRaw().substring(2)).hasSize(32);
    }

    @Test
    public void testPaddingExactly32Chars() {
        // Test with exactly 32 hex chars (no padding needed)
        String userRef = "00000000000000000000000000000001";

        String hexValue = userRef.startsWith("0x") ? userRef.substring(2) : userRef;
        String paddedHex = String.format("%032x", new java.math.BigInteger(hexValue, 16));
        Cloid cloid = Cloid.fromStr("0x" + paddedHex);

        assertThat(cloid.toRaw()).isEqualTo("0x00000000000000000000000000000001");
    }

    @Test
    public void testComparisonWithFromInt() {
        // Verify padding produces same result as fromInt()
        String userRef = "ff";

        String hexValue = userRef.startsWith("0x") ? userRef.substring(2) : userRef;
        String paddedHex = String.format("%032x", new java.math.BigInteger(hexValue, 16));
        Cloid cloidFromPadding = Cloid.fromStr("0x" + paddedHex);

        Cloid cloidFromInt = Cloid.fromInt(255L);

        assertThat(cloidFromPadding).isEqualTo(cloidFromInt);
        assertThat(cloidFromPadding.toRaw()).isEqualTo("0x000000000000000000000000000000ff");
    }
}
