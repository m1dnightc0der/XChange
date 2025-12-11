package org.knowm.xchange.hyperliquid.service;

import org.junit.Test;
import si.mazi.rescu.SynchronizedValueFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Simple test to verify Ethereum address derivation from private key
 */
public class AddressDerivationTest {

    private static final String TEST_PRIVATE_KEY = "0xa12ae31fe37635f851bc41e280b54f406660f2666f26a5c9de0e999274fdebad";

    @Test
    public void testAddressDerivation() {
        SynchronizedValueFactory<Long> mockNonce = mock(SynchronizedValueFactory.class);
        
        HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, mockNonce, false, null,null);
        
        String derivedAddress = auth.getEthereumAddress();
        System.out.println("Derived address: " + derivedAddress);
        
        assertThat(derivedAddress).isNotNull();
        assertThat(derivedAddress).startsWith("0x");
        assertThat(derivedAddress).hasSize(42);
    }
}