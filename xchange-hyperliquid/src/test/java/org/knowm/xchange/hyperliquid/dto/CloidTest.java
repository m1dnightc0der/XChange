package org.knowm.xchange.hyperliquid.dto;

import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CloidTest {

    @Test
    public void testValidCloidFromStr() {
        // Valid 16-byte (32 hex char) cloid
        Cloid cloid = Cloid.fromStr("0x00000000000000000000000000000001");
        assertThat(cloid.toRaw()).isEqualTo("0x00000000000000000000000000000001");
    }

    @Test
    public void testValidCloidFromInt() {
        // Create from integer
        Cloid cloid = Cloid.fromInt(1L);
        assertThat(cloid.toRaw()).isEqualTo("0x00000000000000000000000000000001");

        Cloid cloid2 = Cloid.fromInt(255L);
        assertThat(cloid2.toRaw()).isEqualTo("0x000000000000000000000000000000ff");
    }

    @Test
    public void testInvalidCloidTooShort() {
        // Should reject - only 6 hex chars instead of 32
        assertThatThrownBy(() -> Cloid.fromStr("0x77dd55"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cloid is not 16 bytes");
    }

    @Test
    public void testInvalidCloidNoPrefix() {
        // Should reject - missing 0x prefix
        assertThatThrownBy(() -> Cloid.fromStr("00000000000000000000000000000001"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cloid is not a hex string");
    }

    @Test
    public void testInvalidCloidTooLong() {
        // Should reject - 34 hex chars instead of 32
        assertThatThrownBy(() -> Cloid.fromStr("0x0000000000000000000000000000000001"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cloid is not 16 bytes");
    }

    @Test
    public void testInvalidCloidInvalidHex() {
        // Should reject - contains invalid hex character 'g'
        assertThatThrownBy(() -> Cloid.fromStr("0x0000000000000000000000000000000g"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("invalid hex characters");
    }

    @Test
    public void testInvalidCloidNull() {
        // Should reject null
        assertThatThrownBy(() -> Cloid.fromStr(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cloid cannot be null");
    }

    @Test
    public void testCloidEquality() {
        Cloid cloid1 = Cloid.fromInt(123);
        Cloid cloid2 = Cloid.fromInt(123);
        Cloid cloid3 = Cloid.fromInt(456);

        assertThat(cloid1).isEqualTo(cloid2);
        assertThat(cloid1).isNotEqualTo(cloid3);
        assertThat(cloid1.hashCode()).isEqualTo(cloid2.hashCode());
    }

    @Test
    public void testCloidToString() {
        Cloid cloid = Cloid.fromInt(255);
        assertThat(cloid.toString()).isEqualTo("0x000000000000000000000000000000ff");
    }

    @Test
    public void testToMinimalHex() {
        // Test conversion back to minimal hex string
        Cloid cloid = Cloid.fromStr("0x0000000000000000000000000077dd55");

        assertThat(cloid.toMinimalHex()).isEqualTo("77dd55");
        assertThat(cloid.toMinimalHexWith0x()).isEqualTo("0x77dd55");
    }

    @Test
    public void testToMinimalHexSingleDigit() {
        Cloid cloid = Cloid.fromStr("0x00000000000000000000000000000001");

        assertThat(cloid.toMinimalHex()).isEqualTo("1");
        assertThat(cloid.toMinimalHexWith0x()).isEqualTo("0x1");
    }

    @Test
    public void testToMinimalHexZero() {
        Cloid cloid = Cloid.fromInt(0);

        assertThat(cloid.toMinimalHex()).isEqualTo("0");
        assertThat(cloid.toMinimalHexWith0x()).isEqualTo("0x0");
    }

    @Test
    public void testToMinimalHexLongerValue() {
        Cloid cloid = Cloid.fromStr("0x000000000000000000000000deadbeef");

        assertThat(cloid.toMinimalHex()).isEqualTo("deadbeef");
        assertThat(cloid.toMinimalHexWith0x()).isEqualTo("0xdeadbeef");
    }

    @Test
    public void testRoundTripConversion() {
        // Test that we can convert back and forth
        String original = "77dd55";

        // Pad to create Cloid
        String paddedHex = String.format("%032x", new java.math.BigInteger(original, 16));
        Cloid cloid = Cloid.fromStr("0x" + paddedHex);

        // Convert back to minimal
        assertThat(cloid.toMinimalHex()).isEqualTo(original);
    }

    @Test
    public void testStaticToMinimalHexString() {
        // Test static method with various inputs
        assertThat(Cloid.toMinimalHexString("0x0000000000000000000000000077dd55"))
            .isEqualTo("77dd55");

        assertThat(Cloid.toMinimalHexString("0x00000000000000000000000000000001"))
            .isEqualTo("1");

        assertThat(Cloid.toMinimalHexString("0x000000000000000000000000deadbeef"))
            .isEqualTo("deadbeef");

        // Without 0x prefix
        assertThat(Cloid.toMinimalHexString("0000000000000000000000000077dd55"))
            .isEqualTo("77dd55");
    }

    @Test
    public void testStaticToMinimalHexStringNull() {
        // Should return null for null input
        assertThat(Cloid.toMinimalHexString(null)).isNull();
    }

    @Test
    public void testStaticToMinimalHexStringEmpty() {
        // Should return null for empty string
        assertThat(Cloid.toMinimalHexString("")).isNull();
    }

    @Test
    public void testStaticToMinimalHexStringInvalidHex() {
        // Should reject invalid hex
        assertThatThrownBy(() -> Cloid.toMinimalHexString("0xzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid hex string");
    }

    @Test
    public void testStaticToMinimalHexStringZero() {
        // Test zero value
        assertThat(Cloid.toMinimalHexString("0x00000000000000000000000000000000"))
            .isEqualTo("0");
    }
}
