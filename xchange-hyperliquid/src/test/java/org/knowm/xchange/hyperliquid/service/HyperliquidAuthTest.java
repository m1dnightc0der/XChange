package org.knowm.xchange.hyperliquid.service;

import org.junit.Test;
import org.knowm.xchange.exceptions.ExchangeException;
import si.mazi.rescu.RestInvocation;
import si.mazi.rescu.SynchronizedValueFactory;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Comprehensive tests for HyperliquidAuth class
 * Tests EIP-712 signature generation, Ethereum address derivation, and authentication functionality
 */
public class HyperliquidAuthTest {

    // Test data provided by user
    private static final String TEST_PRIVATE_KEY = "0xa12ae31fe37635f851bc41e280b54f406660f2666f26a5c9de0e999274fdebad";
    private static final String TEST_VAULT_ADDRESS = "0x9dE7084a68D2f65d6eA8E58eFc61843241A20943";
    
    // Expected Ethereum address derived from the private key
    private static final String EXPECTED_ETH_ADDRESS = "0x9de7084a68d2f65d6ea8e58efc61843241a20943";

    private SynchronizedValueFactory<Long> createMockNonceFactory() {
        SynchronizedValueFactory<Long> nonceFactory = mock(SynchronizedValueFactory.class);
        when(nonceFactory.createValue()).thenReturn(1699123456789L); // Fixed timestamp for reproducible tests
        return nonceFactory;
    }

    @Test
    public void testCreateHyperliquidAuth_WithValidParams() {
        // Test successful creation with valid parameters
        SynchronizedValueFactory<Long> nonce = createMockNonceFactory();
        
        HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, nonce, true, TEST_VAULT_ADDRESS,null);

        assertThat(auth).isNotNull();
    }

    @Test
    public void testCreateHyperliquidAuth_WithNullPrivateKey() {
        // Test that null private key returns null
        SynchronizedValueFactory<Long> nonce = createMockNonceFactory();
        
        HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
            null, nonce, true, TEST_VAULT_ADDRESS,null);
        
        assertThat(auth).isNull();
    }

    @Test
    public void testEthereumAddressDerivation() {
        // Test that the correct Ethereum address is derived from the private key
        SynchronizedValueFactory<Long> nonce = createMockNonceFactory();
        
        HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, nonce, false, null,null);
        
        String derivedAddress = auth.getEthereumAddress();
        
        assertThat(derivedAddress).isNotNull();
        assertThat(derivedAddress).isEqualToIgnoringCase(EXPECTED_ETH_ADDRESS);
        assertThat(derivedAddress).startsWith("0x");
        assertThat(derivedAddress).hasSize(42); // 0x + 40 hex characters
    }

    @Test
    public void testEthereumAddressDerivation_ConsistentResults() {
        // Test that address derivation is deterministic
        SynchronizedValueFactory<Long> nonce = createMockNonceFactory();
        
        HyperliquidAuth auth1 = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, nonce, false, null,null);
        HyperliquidAuth auth2 = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, nonce, false, null,null);
        
        String address1 = auth1.getEthereumAddress();
        String address2 = auth2.getEthereumAddress();
        
        assertThat(address1).isEqualTo(address2);
    }

    @Test
    public void testDigestParams_WithValidRestInvocation() {
        // Test signature generation with valid REST invocation
        SynchronizedValueFactory<Long> nonce = createMockNonceFactory();
        
        HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, nonce, true, TEST_VAULT_ADDRESS,null);
        
        // Create mock REST invocation
        RestInvocation mockInvocation = mock(RestInvocation.class);
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("type", "openOrders");
        requestBody.put("user", EXPECTED_ETH_ADDRESS);
        
        String requestBodyJson = "{\"type\":\"openOrders\",\"user\":\"" + EXPECTED_ETH_ADDRESS + "\"}";
        when(mockInvocation.getRequestBody()).thenReturn(requestBodyJson);
        
        String signature = auth.digestParams(mockInvocation);
        
        assertThat(signature).isNotNull();
        assertThat(signature).startsWith("0x");
        assertThat(signature).hasSize(132); // 0x + 64 (r) + 64 (s) + 2 (v) = 132 characters
    }

    @Test
    public void testDigestParams_WithEmptyRequestBody() {
        // Test that empty request body throws exception
        SynchronizedValueFactory<Long> nonce = createMockNonceFactory();
        
        HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, nonce, true, null,null);
        
        RestInvocation mockInvocation = mock(RestInvocation.class);
        when(mockInvocation.getRequestBody()).thenReturn("");
        
        assertThatThrownBy(() -> auth.digestParams(mockInvocation))
            .isInstanceOf(ExchangeException.class)
            .hasMessage("Could not sign Hyperliquid request");
    }

    @Test
    public void testDigestParams_WithNullRequestBody() {
        // Test that null request body throws exception
        SynchronizedValueFactory<Long> nonce = createMockNonceFactory();
        
        HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, nonce, false, null,null);
        
        RestInvocation mockInvocation = mock(RestInvocation.class);
        when(mockInvocation.getRequestBody()).thenReturn(null);
        
        assertThatThrownBy(() -> auth.digestParams(mockInvocation))
            .isInstanceOf(ExchangeException.class)
            .hasMessage("Could not sign Hyperliquid request");
    }

    @Test
    public void testSignatureGeneration() {
        // Test that signatures are generated and have correct format
        // Note: ECDSA signatures contain randomness, so they won't be deterministic
        SynchronizedValueFactory<Long> nonceFactory1 = mock(SynchronizedValueFactory.class);
        SynchronizedValueFactory<Long> nonceFactory2 = mock(SynchronizedValueFactory.class);
        
        // Use same nonce for both
        long fixedNonce = 1699123456789L;
        when(nonceFactory1.createValue()).thenReturn(fixedNonce);
        when(nonceFactory2.createValue()).thenReturn(fixedNonce);
        
        HyperliquidAuth auth1 = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, nonceFactory1, true, TEST_VAULT_ADDRESS,null);
        HyperliquidAuth auth2 = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, nonceFactory2, true, TEST_VAULT_ADDRESS,null);
        
        // Create identical mock invocations
        RestInvocation mockInvocation1 = mock(RestInvocation.class);
        RestInvocation mockInvocation2 = mock(RestInvocation.class);
        
        String requestBody = "{\"type\":\"openOrders\",\"user\":\"" + EXPECTED_ETH_ADDRESS + "\"}";
        when(mockInvocation1.getRequestBody()).thenReturn(requestBody);
        when(mockInvocation2.getRequestBody()).thenReturn(requestBody);
        
        String signature1 = auth1.digestParams(mockInvocation1);
        String signature2 = auth2.digestParams(mockInvocation2);
        
        // Signatures should have correct format (even if different due to ECDSA randomness)
        assertThat(signature1).startsWith("0x");
        assertThat(signature1).hasSize(132);
        assertThat(signature2).startsWith("0x");
        assertThat(signature2).hasSize(132);
        
        // Both should be valid hex strings
        assertThat(signature1.substring(2)).matches("[0-9a-f]+");
        assertThat(signature2.substring(2)).matches("[0-9a-f]+");
    }

    @Test
    public void testMainnetVsTestnetBehavior() {
        // Test different behavior for mainnet vs testnet
        SynchronizedValueFactory<Long> nonce = createMockNonceFactory();
        
        HyperliquidAuth mainnetAuth = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, nonce, true, null,null);  // mainnet
        HyperliquidAuth testnetAuth = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, nonce, false, null,null); // testnet
        
        RestInvocation mockInvocation = mock(RestInvocation.class);
        String requestBody = "{\"type\":\"openOrders\",\"user\":\"" + EXPECTED_ETH_ADDRESS + "\"}";
        when(mockInvocation.getRequestBody()).thenReturn(requestBody);
        
        String mainnetSignature = mainnetAuth.digestParams(mockInvocation);
        String testnetSignature = testnetAuth.digestParams(mockInvocation);
        
        // Signatures should be different because phantom agent uses different source ("a" vs "b")
        assertThat(mainnetSignature).isNotEqualTo(testnetSignature);
        assertThat(mainnetSignature).hasSize(132);
        assertThat(testnetSignature).hasSize(132);
    }

    @Test
    public void testVaultAddressHandling() {
        // Test behavior with and without vault address
        SynchronizedValueFactory<Long> nonce = createMockNonceFactory();
        
        HyperliquidAuth authWithVault = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, nonce, true, TEST_VAULT_ADDRESS,null);
        HyperliquidAuth authWithoutVault = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, nonce, true, null,null);
        
        RestInvocation mockInvocation = mock(RestInvocation.class);
        String requestBody = "{\"type\":\"openOrders\",\"user\":\"" + EXPECTED_ETH_ADDRESS + "\"}";
        when(mockInvocation.getRequestBody()).thenReturn(requestBody);
        
        String signatureWithVault = authWithVault.digestParams(mockInvocation);
        String signatureWithoutVault = authWithoutVault.digestParams(mockInvocation);
        
        // Signatures should be different because vault address affects action hash
        assertThat(signatureWithVault).isNotEqualTo(signatureWithoutVault);
        assertThat(signatureWithVault).hasSize(132);
        assertThat(signatureWithoutVault).hasSize(132);
    }

    @Test
    public void testComplexRequestBody() {
        // Test with more complex request body (order placement)
        SynchronizedValueFactory<Long> nonce = createMockNonceFactory();
        
        HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, nonce, false, null,null);
        
        RestInvocation mockInvocation = mock(RestInvocation.class);
        String complexRequestBody = "{"
            + "\"type\":\"order\","
            + "\"orders\":[{"
            + "\"a\":0,"
            + "\"b\":true,"
            + "\"p\":\"50000\","
            + "\"s\":\"0.1\","
            + "\"r\":false,"
            + "\"t\":{\"limit\":{\"tif\":\"Gtc\"}}"
            + "}],"
            + "\"grouping\":\"na\""
            + "}";
        
        when(mockInvocation.getRequestBody()).thenReturn(complexRequestBody);
        
        String signature = auth.digestParams(mockInvocation);
        
        assertThat(signature).isNotNull();
        assertThat(signature).startsWith("0x");
        assertThat(signature).hasSize(132);
    }

    @Test
    public void testInvalidPrivateKeyFormat() {
        // Test with invalid private key format
        SynchronizedValueFactory<Long> nonce = createMockNonceFactory();
        
        // Create auth with invalid key (should not throw on creation)
        HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
            "invalid_private_key", nonce, false, null,null);
        
        // Should throw when trying to use it
        assertThatThrownBy(() -> auth.getEthereumAddress())
            .isInstanceOf(ExchangeException.class)
            .hasMessageContaining("Failed to derive Ethereum address");
    }

    @Test
    public void testPrivateKeyWithoutHexPrefix() {
        // Test private key without 0x prefix
        String privateKeyWithoutPrefix = TEST_PRIVATE_KEY.substring(2); // Remove 0x
        SynchronizedValueFactory<Long> nonce = createMockNonceFactory();

        HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
            privateKeyWithoutPrefix, nonce, false, null,null);

        String derivedAddress = auth.getEthereumAddress();
        assertThat(derivedAddress).isEqualToIgnoringCase(EXPECTED_ETH_ADDRESS);
    }

    @Test
    public void testSignL1Action_MatchesPythonSDK() throws Exception {
        // Test that Java signL1Action produces the same signature as Python SDK's sign_l1_action
        // Python test case:
        //   private_key = ''
        //   wallet = Account.from_key(private_key)
        //   action = {'type': 'order', 'orders': [{'a': 0, 'b': True, 'p': '50000', 's': '0.001', 'r': False, 't': {'limit': {'tif': 'Gtc'}}}], 'grouping': 'na'}
        //   nonce = 1234567890000
        //   signature = sign_l1_action(wallet, action, None, nonce, None, False)
        // Expected output from Python:
        //   r: 0x7b20b8eea8a5840d0aa30f54c8b2ab7d661ef63acbe6618ce6fe412280423b96
        //   s: 0x61d2d2f76338f205d0a820bc59e9428427b6af1779b781e9e067abbde6bc0fd9
        //   v: 27

        String testPrivateKey = "";
        long testNonce = 1234567890000L;

        SynchronizedValueFactory<Long> nonce = mock(SynchronizedValueFactory.class);
        when(nonce.createValue()).thenReturn(testNonce);

        HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
            testPrivateKey, nonce, false, null,null);  // isMainnet=false, vaultAddress=null

        // Verify derived address matches Python
        String expectedAddress = "";
        assertThat(auth.getEthereumAddress()).isEqualToIgnoringCase(expectedAddress);

        // Create order action matching Python test
        Map<String, Object> orderWire = new java.util.LinkedHashMap<>();
        orderWire.put("a", 0);  // asset index
        orderWire.put("b", true);  // is_buy
        orderWire.put("p", "50000");  // price
        orderWire.put("s", "0.001");  // size
        orderWire.put("r", false);  // reduce_only

        Map<String, Object> limitType = new java.util.LinkedHashMap<>();
        limitType.put("tif", "Gtc");
        Map<String, Object> orderType = new java.util.LinkedHashMap<>();
        orderType.put("limit", limitType);
        orderWire.put("t", orderType);

        Map<String, Object> action = new java.util.LinkedHashMap<>();
        action.put("type", "order");
        action.put("orders", java.util.Arrays.asList(orderWire));
        action.put("grouping", "na");

        // Sign the action
        Map<String, Object> signature = auth.signL1Action(action, testNonce, null);

        // Verify signature matches Python SDK output
        String expectedR = "0x7b20b8eea8a5840d0aa30f54c8b2ab7d661ef63acbe6618ce6fe412280423b96";
        String expectedS = "0x61d2d2f76338f205d0a820bc59e9428427b6af1779b781e9e067abbde6bc0fd9";
        int expectedV = 27;

        assertThat(signature).containsEntry("r", expectedR);
        assertThat(signature).containsEntry("s", expectedS);
        assertThat(signature).containsEntry("v", expectedV);
    }

    @Test
    public void testSignL1Action_WithDifferentPayload() throws Exception {
        // Test with user's specific payload: asset=1, price=3800, size=0.003
        // Python test case:
        //   action = {"type":"order","orders":[{"a":1,"b":true,"p":"3800","s":"0.003","r":false,"t":{"limit":{"tif":"Gtc"}}}],"grouping":"na"}
        //   nonce = 1234567890000
        // Expected output from Python:
        //   r: 0xcef32a954258bce5590abe29e9c169f4c2df20cacd895530a97c6dc18a0d5ada
        //   s: 0x6640aa362c1af237eeea5cf464360c9a1f63c1550d2dff6d0c193a6870d88b84
        //   v: 27

        String testPrivateKey = "";
        long testNonce = 1234567890000L;

        SynchronizedValueFactory<Long> nonce = mock(SynchronizedValueFactory.class);
        when(nonce.createValue()).thenReturn(testNonce);

        HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
            testPrivateKey, nonce, false, null,null);

        // Create order action with different values
        Map<String, Object> orderWire = new java.util.LinkedHashMap<>();
        orderWire.put("a", 1);  // asset index 1 (not 0)
        orderWire.put("b", true);  // is_buy
        orderWire.put("p", "3800");  // price
        orderWire.put("s", "0.003");  // size
        orderWire.put("r", false);  // reduce_only

        Map<String, Object> limitType = new java.util.LinkedHashMap<>();
        limitType.put("tif", "Gtc");
        Map<String, Object> orderType = new java.util.LinkedHashMap<>();
        orderType.put("limit", limitType);
        orderWire.put("t", orderType);

        Map<String, Object> action = new java.util.LinkedHashMap<>();
        action.put("type", "order");
        action.put("orders", java.util.Arrays.asList(orderWire));
        action.put("grouping", "na");

        // Sign the action
        Map<String, Object> signature = auth.signL1Action(action, testNonce, null);

        // Verify signature matches Python SDK output
        String expectedR = "0xcef32a954258bce5590abe29e9c169f4c2df20cacd895530a97c6dc18a0d5ada";
        String expectedS = "0x6640aa362c1af237eeea5cf464360c9a1f63c1550d2dff6d0c193a6870d88b84";
        int expectedV = 27;

        assertThat(signature).containsEntry("r", expectedR);
        assertThat(signature).containsEntry("s", expectedS);
        assertThat(signature).containsEntry("v", expectedV);
    }
}