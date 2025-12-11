package org.knowm.xchange.hyperliquid.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import si.mazi.rescu.SynchronizedValueFactory;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test to compare Java signature generation with Python SDK
 */
public class SignatureComparisonTest {

    private static final String TEST_PRIVATE_KEY = "";
    private static final String TEST_VAULT_ADDRESS = "";
    private static final long FIXED_NONCE = 1234567890123L;

    @Test
    public void testSignatureMatchesPython() throws Exception {
        // Create auth with test credentials
        SynchronizedValueFactory<Long> nonceFactory = mock(SynchronizedValueFactory.class);
        when(nonceFactory.createValue()).thenReturn(FIXED_NONCE);

        HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, nonceFactory, false, TEST_VAULT_ADDRESS,null);

        // Create order action matching Python test
        // IMPORTANT: Use LinkedHashMap to preserve insertion order for MessagePack
        Map<String, Object> orderWire = new java.util.LinkedHashMap<>();
        orderWire.put("a", 0);  // ETH
        orderWire.put("b", true);  // is_buy
        orderWire.put("p", "3000.0");  // price
        orderWire.put("s", "0.1");  // size
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
        Map<String, Object> signature = auth.signL1Action(action, FIXED_NONCE, null);

        // Print for comparison
        System.out.println("Wallet address: " + auth.getEthereumAddress());
        System.out.println("\nAction:");
        ObjectMapper mapper = new ObjectMapper();
        System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(action));

        System.out.println("\nNonce: " + FIXED_NONCE);
        System.out.println("Vault Address: " + TEST_VAULT_ADDRESS);

        System.out.println("\nSignature:");
        System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(signature));

        // Expected signature from Python
        System.out.println("\nExpected from Python:");
        System.out.println("r: 0x907d182ee0fdc94d68f3bb39ca48d24ca09c8e4c31fdfe07c11f111e2d7cee84");
        System.out.println("s: 0x6658a51aae2af620b8218aa1f6a09fddff61253f1ab5cfb58f5f56b2341d881");
        System.out.println("v: 27");
    }
}
