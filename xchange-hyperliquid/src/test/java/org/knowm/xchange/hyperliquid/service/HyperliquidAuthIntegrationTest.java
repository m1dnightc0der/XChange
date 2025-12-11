package org.knowm.xchange.hyperliquid.service;

import org.junit.Test;
import si.mazi.rescu.RestInvocation;
import si.mazi.rescu.SynchronizedValueFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration test validating HyperliquidAuth with the specific credentials provided by user
 * Vault Address: 0x9dE7084a68D2f65d6eA8E58eFc61843241A20943
 * Secret Key: 0xa12ae31fe37635f851bc41e280b54f406660f2666f26a5c9de0e999274fdebad
 */
public class HyperliquidAuthIntegrationTest {

    private static final String USER_PROVIDED_SECRET_KEY = "0xa12ae31fe37635f851bc41e280b54f406660f2666f26a5c9de0e999274fdebad";
    private static final String USER_PROVIDED_VAULT_ADDRESS = "0x9dE7084a68D2f65d6eA8E58eFc61843241A20943";

    @Test
    public void testUserProvidedCredentials_MainnetWithVault() {
        // Test with user-provided credentials on mainnet with vault
        SynchronizedValueFactory<Long> nonce = mock(SynchronizedValueFactory.class);
        when(nonce.createValue()).thenReturn(1699123456789L);
        
        HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
            USER_PROVIDED_SECRET_KEY, nonce, true, USER_PROVIDED_VAULT_ADDRESS,null);
        
        assertThat(auth).isNotNull();
        
        // Verify address derivation matches vault address
        String derivedAddress = auth.getEthereumAddress();
        assertThat(derivedAddress.toLowerCase()).isEqualTo(USER_PROVIDED_VAULT_ADDRESS.toLowerCase());
        
        // Test signature generation for openOrders request
        RestInvocation mockInvocation = mock(RestInvocation.class);
        String requestBody = "{\"type\":\"openOrders\",\"user\":\"" + derivedAddress + "\"}";
        when(mockInvocation.getRequestBody()).thenReturn(requestBody);
        
        String signature = auth.digestParams(mockInvocation);
        
        // Validate signature format
        assertThat(signature).startsWith("0x");
        assertThat(signature).hasSize(132); // 0x + 64(r) + 64(s) + 2(v) = 132
        assertThat(signature.substring(2)).matches("[0-9a-f]+");
        
        System.out.println("Generated signature for mainnet with vault: " + signature);
    }

    @Test
    public void testUserProvidedCredentials_TestnetWithoutVault() {
        // Test with user-provided secret key on testnet without vault
        SynchronizedValueFactory<Long> nonce = mock(SynchronizedValueFactory.class);
        when(nonce.createValue()).thenReturn(1699123456789L);
        
        HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
            USER_PROVIDED_SECRET_KEY, nonce, false, null,null);
        
        assertThat(auth).isNotNull();
        
        // Verify address derivation
        String derivedAddress = auth.getEthereumAddress();
        assertThat(derivedAddress.toLowerCase()).isEqualTo(USER_PROVIDED_VAULT_ADDRESS.toLowerCase());
        
        // Test signature generation for getUserState request
        RestInvocation mockInvocation = mock(RestInvocation.class);
        String requestBody = "{\"type\":\"clearinghouseState\",\"user\":\"" + derivedAddress + "\"}";
        when(mockInvocation.getRequestBody()).thenReturn(requestBody);
        
        String signature = auth.digestParams(mockInvocation);
        
        // Validate signature format
        assertThat(signature).startsWith("0x");
        assertThat(signature).hasSize(132);
        assertThat(signature.substring(2)).matches("[0-9a-f]+");
        
        System.out.println("Generated signature for testnet without vault: " + signature);
    }

    @Test
    public void testCompleteWorkflow_PlaceOrder() {
        // Test complete workflow with order placement
        SynchronizedValueFactory<Long> nonce = mock(SynchronizedValueFactory.class);
        when(nonce.createValue()).thenReturn(1699123456789L);
        
        HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
            USER_PROVIDED_SECRET_KEY, nonce, true, USER_PROVIDED_VAULT_ADDRESS,null);
        
        String derivedAddress = auth.getEthereumAddress();
        
        // Test with complex order placement request
        RestInvocation mockInvocation = mock(RestInvocation.class);
        String orderRequestBody = "{"
            + "\"type\":\"order\","
            + "\"orders\":[{"
            + "\"a\":0,"
            + "\"b\":true,"
            + "\"p\":\"50000.0\","
            + "\"s\":\"0.001\","
            + "\"r\":false,"
            + "\"t\":{\"limit\":{\"tif\":\"Gtc\"}}"
            + "}],"
            + "\"grouping\":\"na\""
            + "}";
        
        when(mockInvocation.getRequestBody()).thenReturn(orderRequestBody);
        
        String signature = auth.digestParams(mockInvocation);
        
        assertThat(signature).isNotNull();
        assertThat(signature).startsWith("0x");
        assertThat(signature).hasSize(132);
        
        System.out.println("Generated signature for order placement: " + signature);
        System.out.println("User address: " + derivedAddress);
        System.out.println("Vault address: " + USER_PROVIDED_VAULT_ADDRESS);
        System.out.println("Addresses match: " + derivedAddress.toLowerCase().equals(USER_PROVIDED_VAULT_ADDRESS.toLowerCase()));
    }
}